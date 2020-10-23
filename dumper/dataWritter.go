package dumper

import (
	"database/sql"
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
	"strings"
)

func PrintTableDataOrdered(db *sql.DB, tables []schemareader.Table, data DataDumper) int {
	tableMap := make(map[string]schemareader.Table)
	fmt.Println("BEGIN;")
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	result := printTableData(db, tableMap, data, tableMap["rhnchannel"], make(map[string]bool), make([]string, 0))
	fmt.Println("COMMIT;")

	return result
}

func printTableData(db *sql.DB, tableMap map[string]schemareader.Table, data DataDumper, table schemareader.Table, processedTables map[string]bool, path []string) int {

	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

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
		result = result + printTableData(db, tableMap, data, tableReference, processedTables, path)
	}
	for _, key := range tableData.Keys {

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for column, value := range key.key {
			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", column, len(whereParameters)+1))
			scanParameters = append(scanParameters, value)
		}
		formattedColumns := strings.Join(table.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")

		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, table.Name, formatedWhereParameters)
		rows := executeQueryWithResults(db, sql, scanParameters...)

		for _, row := range rows {
			result++
			fmt.Println(prepareRowInsert(db, table, row, tableMap, columnIndexes))
		}
	}

	for _, reference := range table.ReferencedBy {
		tableReference, ok := tableMap[reference.TableName]
		if !ok {
			continue
		}
		if !shouldFollowReferenceByLink(path, table, reference, tableReference) {
			continue
		}
		result = result + printTableData(db, tableMap, data, tableReference, processedTables, path)
	}
	return result
}
