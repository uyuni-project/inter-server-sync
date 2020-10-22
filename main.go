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
	} else if len(os.Args) > 1 && strings.Compare(os.Args[1], "v2") == 0 {
		channelLabels := []int{117}
		filters := dumper.DumpTableFilter(db, tables, channelLabels)

		if len(os.Args) > 2 && strings.Compare(os.Args[1], "ids") == 0 {
			for _, value := range filters.TableKeys {
				fmt.Printf("key: %s \n count: %d \n values: %s\n", value.TableName, len(value.Keys), value)
			}

			fmt.Printf("################%d\n\n", len(filters.Queries))
		}
		fmt.Println("BEGIN;")
		for _, value := range filters.Queries {
			fmt.Println(value)
		}
		fmt.Println("COMMIT;")
	} else {
		fmt.Println("BEGIN;")
		for _, query := range dumper.Dump(db, tables) {
			fmt.Println(query + "\n")
		}
		fmt.Println("COMMIT;")
	}

}
