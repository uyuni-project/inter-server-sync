package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/cli"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"os"
	"runtime/pprof"
)

func loginit() {
	Logfile := "/tmp/uyuni_iss_log.json"
	lf, err := os.OpenFile(Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if os.IsNotExist(err) {
		f, err := os.Create(Logfile)
		if err != nil {
			log.Error().Msg("Unable to create logfile")
		}
		lf = f
	}
	multi := zerolog.MultiLevelWriter(lf, os.Stdout)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

}

func main() {
	loginit()
	parsedArgs, err := cli.CliArgs(os.Args)
	if err != nil {
		log.Error().Msg("Not enough arguments")
		os.Exit(1)
	}
	level, err := zerolog.ParseLevel(parsedArgs.Loglevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	if parsedArgs.Cpuprofile != "" {
		f, err := os.Create(parsedArgs.Cpuprofile)
		if err != nil {
			log.Fatal().Msg("could not create CPU profile: ")
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal().Msg("could not start CPU profile: ")
		}
		defer pprof.StopCPUProfile()
	}

	db := schemareader.GetDBconnection(parsedArgs.Config)
	defer db.Close()

	if parsedArgs.Dot {
		tables := schemareader.ReadTablesSchema(db, entityDumper.SoftwareChannelTableNames())
		schemareader.DumpToGraphviz(tables)
		return
	}
	if len(parsedArgs.ChannleLabels) > 0 {
		channelLabels := parsedArgs.ChannleLabels
		outputFolder := parsedArgs.Path
		tableData := entityDumper.DumpChannelData(db, channelLabels, outputFolder)

		if parsedArgs.Debug {
			for index, channelTableData := range tableData {
				fmt.Printf("###Processing channe%d...", index)
				for path := range channelTableData.Paths {
					println(path)
				}
				count := 0
				for _, value := range channelTableData.TableData {
					fmt.Printf("%s number inserts: %d \n\t %s keys: %s\n", value.TableName, len(value.Keys),
						value.TableName, value.Keys)
					count = count + len(value.Keys)
				}
				fmt.Printf("IDS############%d\n\n", count)
			}

		}
	}
	if parsedArgs.Memprofile != "" {
		f, err := os.Create(parsedArgs.Memprofile)
		if err != nil {
			log.Fatal().Msg("could not create memory profile: ")
		}
		defer f.Close() // error handling omitted for example
		//runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal().Msg("could not write memory profile: ")
		}
	}

}
