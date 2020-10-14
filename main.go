package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

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

		if len(table.PKSequence) > 0 {
			fmt.Printf("\"%s-id-%s\" [label=\"%s\" shape=note];\n", table.Name, table.PKSequence, table.PKSequence)
			fmt.Printf("\"%s-id\" -- \"%s-id-%s\" [style=dashed];\n", table.Name, table.Name, table.PKSequence)
		}

		for _, index := range table.UniqueIndexes {
			label := "unique"
			if table.MainUniqueIndex != nil {
				if strings.Compare(index.Name, table.MainUniqueIndex.Name) == 0 {
					label = "unique main"
				}
			}
			fmt.Printf("\"%s\" [label=\"%s\" shape=tab];\n", index.Name, label)

			for _, indexColumn := range index.Columns {
				fmt.Printf("\"%s-%s\" -- \"%s\" [style=dashed];\n", table.Name, indexColumn, index.Name)
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
