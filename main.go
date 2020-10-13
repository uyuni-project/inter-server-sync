package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/moio/mgr-dump/schemareader"
)

// cd spacewalk/java; make -f Makefile.docker dockerrun_pg
const connectionString = "user='spacewalk' password='spacewalk' dbname='susemanager' host='localhost' port='5432' sslmode=disable"

// psql --host=localhost --port=5432 --username=spacewalk susemanager

// go run . | dot -Tx11
func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	tables := schemareader.ReadTables(db)

	fmt.Printf("graph schema {\n")
	fmt.Printf("  layout=fdp;")
	fmt.Printf("  mindist=0.3;")

	for _, table := range tables {
		fmt.Printf("\"%s\" [shape=box];\n", table.Name)

		for _, column := range table.Columns {
			_, primary := table.PKColumns[column]
			color := "transparent"
			if primary {
				color = "gainsboro"
			}
			fmt.Printf("\"%s-%s\" [label=\"\" xlabel=\"%s\" style=filled fillcolor=\"%s\"];\n", table.Name, column, column, color)
			fmt.Printf("\"%s\" -- \"%s-%s\";\n", table.Name, table.Name, column)
		}

		for _, index := range table.UniqueIndexes {
			fmt.Printf("\"%s\" [shape=doublecircle];\n", index.Name)

			for _, indexColumn := range index.Columns {
				fmt.Printf("\"%s-%s\" -- \"%s\";\n", table.Name, indexColumn, index.Name)
			}
		}
	}

	fmt.Printf("}")
}
