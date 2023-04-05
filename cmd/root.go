package cmd

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/uyuni-project/inter-server-sync/utils"
	"log/syslog"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "inter-server-sync",
	Short:   "Uyuni Inter Server Sync tool",
	Version: "0.2.7",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

var cfgFile string
var logLevel string
var serverConfig string
var cpuProfile string
var memProfile string

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Location of configuration file")
	rootCmd.PersistentFlags().String("logLevel", "error", "application log level")
	rootCmd.PersistentFlags().String("serverConfig", "/etc/rhn/rhn.conf", "Server configuration file")
	rootCmd.PersistentFlags().String("cpuProfile", "", "cpuProfile export folder location")
	rootCmd.PersistentFlags().String("memProfile", "", "memProfile export folder location")

	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Warn().Err(err).Msg("Failed to bind PFlags")
	}

	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logLevel = viper.GetString("logLevel")
		cpuProfile = viper.GetString("cpuProfile")
		memProfile = viper.GetString("memProfile")
		serverConfig = viper.GetString("serverConfig")

		logInit()
		cpuProfileInit()
		memProfileDump()
	}
	rootCmd.PersistentPostRun = func(cmd *cobra.Command, args []string) {

		cpuProfileTearDown()
	}
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(utils.GetAbsPath(cfgFile))

		if err := viper.ReadInConfig(); err != nil {
			log.Panic().Err(err).Msg("Failed to read config file")
		}
	}
}

func logCallerMarshalFunction(file string, line int) string {
	paths := strings.Split(file, "/")
	callerFile := file
	foundSubDir := false
	for _, currentPath := range paths {
		if foundSubDir {
			if callerFile != "" {
				callerFile = callerFile + "/"
			}
			callerFile = callerFile + currentPath
		} else {
			if strings.Contains(currentPath, "inter-server-sync") {
				foundSubDir = true
				callerFile = ""
			}
		}
	}
	return callerFile + ":" + strconv.Itoa(line)
}

func logInit() {
	syslogger, err := syslog.New(syslog.LOG_INFO|syslog.LOG_DEBUG|syslog.LOG_WARNING|syslog.LOG_ERR, "inter-server-sync")

	syslogwriter := zerolog.SyslogLevelWriter(syslogger)

	multi := zerolog.MultiLevelWriter(syslogwriter, os.Stdout)
	log.Logger = zerolog.New(multi).With().Timestamp().Caller().Logger()
	zerolog.CallerMarshalFunc = logCallerMarshalFunction
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	log.Info().Msg("Inter server sync started")
}

func cpuProfileInit() {
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile + "end_cpu_profile.prof")
		if err != nil {
			log.Error().Err(err).Msg("could not create CPU profile: ")
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Panic().Err(err).Msg("could not start CPU profile: ")
		}
	}
}

func cpuProfileTearDown() {
	if cpuProfile != "" {
		pprof.StopCPUProfile()
	}
}

func memProfileDump() {
	if log.Debug().Enabled() && len(memProfile) > 0 {

		go func() {

			count := 0
			for {
				time.Sleep(30 * time.Second)
				fileName := fmt.Sprintf("%s/memory_profile_%d.prof", memProfile, count)
				f, err := os.Create(fileName)
				if err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("could not create memory profile file: %s", fileName))
					break
				}
				if err := pprof.WriteHeapProfile(f); err != nil {
					log.Error().Err(err).Msg("could not write memory profile: ")
				}
				f.Close()
				count++
			}
		}()
	}
}
