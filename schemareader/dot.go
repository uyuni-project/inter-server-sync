package schemareader

import (
	"fmt"
	"strings"
)

// DumpToGraphviz outputs a dot representation of a schema. Use:
// go run . | dot -Tx11
func DumpToGraphviz(tables map[string]Table) {
	fmt.Printf("graph schema {\n")
	fmt.Printf("  layout=fdp;\n")
	fmt.Printf("  K=0.15;\n")
	fmt.Printf("  maxiter=1000;\n")
	fmt.Printf("  start=0;\n\n")

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
			if len(table.MainUniqueIndexName) > 0 {
				if strings.Compare(index.Name, table.MainUniqueIndexName) == 0 {
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
