// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"
	"path"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Version = "0.0.0"

var rootCmd = &cobra.Command{
	Use:     "inter-server-sync",
	Short:   "Uyuni Inter Server Sync tool",
	Version: Version,
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

// var cfgFile string
var logLevel string
var serverConfig string
var cpuProfile string
var memProfile string

// The default directory where log files are written.
const logDir = "/var/log/hub/"
const logFileName = "iss2.log"
const globalLogPath = logDir + logFileName

func init() {
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logInit()
		cpuProfileInit()
		memProfileDump()
	}
	rootCmd.PersistentPostRun = func(cmd *cobra.Command, args []string) {
		cpuProfileTearDown()
	}
	rootCmd.PersistentFlags().StringVar(&logLevel, "logLevel", "error", "application log level")
	rootCmd.PersistentFlags().StringVar(&serverConfig, "serverConfig", "/etc/rhn/rhn.conf", "Server configuration file")
	rootCmd.PersistentFlags().StringVar(&cpuProfile, "cpuProfile", "", "cpuProfile export folder location")
	rootCmd.PersistentFlags().StringVar(&memProfile, "memProfile", "", "memProfile export folder location")
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

func getFileWriter() *lumberjack.Logger {
	logPath := globalLogPath
	if file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600); err != nil {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logPath = path.Join(".", logFileName)
		} else {
			logPath = path.Join(homeDir, logFileName)
		}
	} else {
		file.Close()
	}

	fileLogger := lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     7,
		Compress:   true,
	}
	return &fileLogger
}

func logInit() {
	fileWriter := getFileWriter()
	multi := zerolog.MultiLevelWriter(fileWriter, os.Stdout)
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
