package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"github.com/moio/mgr-dump/dumper"
	"github.com/moio/mgr-dump/schemareader"
)

const (
	usage = `ISS export tool

Usage: %s [Options]
	

Options:
`
)

type Args struct {
	channleLabels []string
	path          string
	config        string
	dot           bool
	debug         bool
}

const configFilePath = "/etc/rhn/rhn.conf"

func cli(args []string) (*Args, error) {

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), usage, os.Args[0])
		flag.PrintDefaults()
	}
	channelLabels := flag.String("channels", "", "Labels for channels to sync (comma seprated in case of multiple)")

	path := flag.String("path", ".", "Location for generated data")

	config := flag.String("config", "/etc/rhn/rhn.conf", "Path for the config file")

	dot := flag.Bool("dot", false, "Create dot file for Graphviz")

	debug := flag.Bool("debug", false, "debug export data")

	if len(args) < 2 {
		flag.Usage()
		return nil, errors.New("Insufficent arguments")
	}

	flag.Parse()

	return &Args{strings.Split(*channelLabels, ","), *path, *config, *dot, *debug}, nil
}

func main() {
	parsedArgs, err := cli(os.Args)
	if err != nil {
		os.Exit(1)
	}
	connectionString := schemareader.GetConnectionString(parsedArgs.config)
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if parsedArgs.dot {
		tables := schemareader.ReadAllTablesSchema(db)
		schemareader.DumpToGraphviz(tables)
		return
	}
	if len(parsedArgs.channleLabels) > 0 {
		channelLabels := parsedArgs.channleLabels
		outputFolder := parsedArgs.path
		tableData := dumper.DumpeChannelData(db, channelLabels, outputFolder)

		if parsedArgs.debug {
			for path := range tableData.Paths {
				println(path)
			}
			count := 0
			for _, value := range tableData.TableData {
				fmt.Printf("Table: %s \n\tKeys len: %d \n\t keys: %s\n", value.TableName, len(value.Keys), value.Keys)
				count = count + len(value.Keys)
			}
			fmt.Printf("IDS############%d\n\n", count)
		}
	}
}
