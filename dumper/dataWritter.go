package dumper

import (
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
)

func PrintTableDataOrdered(tables []schemareader.Table, data DataDumper) int {
	tableMap := make(map[string]schemareader.Table)
	fmt.Println("BEGIN;")
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	result := printTableData(tableMap, data, tableMap["rhnchannel"], make(map[string]bool), make([]string, 0))
	fmt.Println("COMMIT;")

	return result
}

func printTableData(tableMap map[string]schemareader.Table, data DataDumper, table schemareader.Table, processedTables map[string]bool, path []string) int {
	result := 0
	_, tableProcessed := processedTables[table.Name]
	processedTables[table.Name] = true
	path = append(path, table.Name)

	tableData, dataOK := data.TableData[table.Name]
	if !dataOK || tableProcessed {
		return result
	}

	for _, reference := range table.References {
		tableReference, ok := tableMap[reference.TableName]
		if !ok {
			continue
		}
		result = result + printTableData(tableMap, data, tableReference, processedTables, path)
	}
	for _, query := range tableData.Queries {
		result++
		println(query)
	}

	for _, reference := range table.ReferencedBy {
		tableReference, ok := tableMap[reference.TableName]
		if !ok {
			continue
		}
		if !shouldFollowReferenceByLink(path, table, reference, tableReference) {
			continue
		}
		result = result + printTableData(tableMap, data, tableReference, processedTables, path)
	}
	return result
}
