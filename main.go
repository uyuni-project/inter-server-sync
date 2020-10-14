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
	fmt.Printf("  K=0.15;")
	fmt.Printf("  maxiter=1000;")
	fmt.Printf("  start=0;")

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
			color := "transparent"
			if index.Main {
				color = "green"
			}
			fmt.Printf("\"%s\" [label=\"\" shape=doublecircle style=filled fillcolor=\"%s\"];\n", index.Name, color)

			for _, indexColumn := range index.Columns {
				fmt.Printf("\"%s-%s\" -- \"%s\";\n", table.Name, indexColumn, index.Name)
			}
		}

		for i, reference := range table.References {
			fmt.Printf("\"%s-%s-%d\" [label=\"\" shape=diamond];\n", table.Name, reference.TableName, i)

			for column, foreignColumn := range reference.ColumnMapping {
				fmt.Printf("\"%s-%s-%d\" -- \"%s-%s\";\n", table.Name, reference.TableName, i, table.Name, column)
				fmt.Printf("\"%s-%s-%d\" -- \"%s-%s\";\n", table.Name, reference.TableName, i, reference.TableName, foreignColumn)
			}
		}
	}

	fmt.Printf("}")
}
