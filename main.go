package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"github.com/moio/mgr-dump/dumper"
	"github.com/moio/mgr-dump/schemareader"
)

// cd spacewalk/java; make -f Makefile.docker dockerrun_pg
const connectionString = "user='spacewalk' password='spacewalk' dbname='susemanager' host='localhost' port='5432' sslmode=disable"

// psql --host=localhost --port=5432 --username=spacewalk susemanager

func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	tables := schemareader.ReadTablesSchema(db)

	if len(os.Args) > 1 && strings.Compare(os.Args[1], "dot") == 0 {
		schemareader.DumpToGraphviz(tables)
	} else {
		//channelLabels := []int{118} // c1
		channelLabels := []int{117, 118, 102, 101, 119, 103, 104} //c2
		//channelLabels := []int{117, 118, 102, 101, 119, 103, 104, 107} //c3
		//channelLabels := []int{117, 118, 102, 101, 119, 103, 104, 107, 108} //c4
		//channelLabels := []int{102, 101, 119, 117, 103, 104, 118, 107, 108, 106} // c5
		//channelLabels := []int{102, 103, 104, 105, 106, 107, 108} // c6
		//channelLabels := []int{117, 118} // c7
		//channelLabels := []int{108} // c8

		//channelLabels := []int{102} // Moio's tests
		tableData := dumper.DumpTableData(db, tables, channelLabels)

		countQueries := dumper.PrintTableDataOrdered(db, tables, tableData)

		if len(os.Args) > 1 && strings.Compare(os.Args[1], "info") == 0 {
			for path, _ := range tableData.Paths {
				println(path)
			}
			count := 0
			for _, value := range tableData.TableData {
				fmt.Printf("Table: %s \n\tQueries len: %d \n\tKeys len: %d \n\t keys: %s\n", value.TableName, len(value.Queries), len(value.Keys), value.Keys)
				count = count + len(value.Keys)
			}

			fmt.Printf("IDS############%d\n\n", count)
			fmt.Printf("countQueries############%d\n\n", countQueries)
		}
	}
}
