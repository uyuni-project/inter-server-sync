package main

import (
	"fmt"
	"os"

	_ "github.com/lib/pq"
	"github.com/uyuni-project/inter-server-sync/cli"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

func main() {
	parsedArgs, err := cli.CliArgs(os.Args)
	if err != nil {
		os.Exit(1)
	}

	db := schemareader.GetDBconnection(parsedArgs.Config)
	defer db.Close()

	if parsedArgs.Dot {
		tables := schemareader.ReadAllTablesSchema(db)
		schemareader.DumpToGraphviz(tables)
		return
	}
	if len(parsedArgs.ChannleLabels) > 0 {
		channelLabels := parsedArgs.ChannleLabels
		outputFolder := parsedArgs.Path
		tableData := dumper.DumpeChannelData(db, channelLabels, outputFolder)

		if parsedArgs.Debug {
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
