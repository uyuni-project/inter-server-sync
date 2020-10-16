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
	tables := schemareader.ReadTables(db)

	if len(os.Args) > 1 && strings.Compare(os.Args[1], "dot") == 0 {
		schemareader.DumpToGraphviz(tables)
	} else {
		for _, query := range dumper.Dump(db, tables) {
			fmt.Println(query + "\n")
		}
	}

}
