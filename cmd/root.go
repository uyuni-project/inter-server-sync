package cmd

import (
	"log/syslog"
	"os"
	"runtime/pprof"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "inter-server-sync",
	Short:   "Uyuni Inter Server Sync tool",
	Version: "0.0.3",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

//var cfgFile string
var logLevel string
var serverConfig string
var cpuProfile string
var memProfile string

func init() {
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logInit()
		cpuProfileInit()
	}
	rootCmd.PersistentPostRun = func(cmd *cobra.Command, args []string) {
		cpuProfileTearDown()
		memProfileDump()
	}
	rootCmd.PersistentFlags().StringVar(&logLevel, "logLevel", "error", "application log level")
	rootCmd.PersistentFlags().StringVar(&serverConfig, "serverConfig", "/etc/rhn/rhn.conf", "Server configuration file")
	rootCmd.PersistentFlags().StringVar(&cpuProfile, "cpuProfile", "", "cpuProfile file location")
	rootCmd.PersistentFlags().StringVar(&memProfile, "memProfile", "", "memProfile file location")
}

func logInit() {
	syslogger, err := syslog.New(syslog.LOG_INFO|syslog.LOG_DEBUG|syslog.LOG_WARNING|syslog.LOG_ERR, "inter-server-sync")

	syslogwriter := zerolog.SyslogLevelWriter(syslogger)

	multi := zerolog.MultiLevelWriter(syslogwriter, os.Stdout)
	log.Logger = zerolog.New(multi).With().Timestamp().Caller().Logger()

	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	// TODO remove next lines after some tests
	log.Info().Msg("Hello Info")
	log.Debug().Msg("Hello Debug")
	log.Trace().Msg("Hello Trace")
}

func cpuProfileInit() {
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Error().Err(err).Msg("could not create CPU profile: ")
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Error().Err(err).Msg("could not start CPU profile: ")
			panic(err)
		}
	}
}
func cpuProfileTearDown() {
	if cpuProfile != "" {
		pprof.StopCPUProfile()
	}
}

func memProfileDump() {
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Error().Err(err).Msg("could not create memory profile: ")
		}
		defer f.Close() // error handling omitted for example
		//runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Error().Err(err).Msg("could not write memory profile: ")
		}
	}
}
