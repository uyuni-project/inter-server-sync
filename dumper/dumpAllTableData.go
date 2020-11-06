package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"strings"
)

func dumpAllTablesData(db *sql.DB, writter *bufio.Writer, schemaMetadata map[string]schemareader.Table, startingTables []schemareader.Table) {
	processedTables := make(map[string]bool)
	// exporting from the starting tables.
	for _, startingTable := range startingTables{
		processedTables = printAllTableData(db, writter, schemaMetadata, startingTable, processedTables, make([]string, 0))
	}
	// Export tables not touch by the starting tables
	for schemaTableName, schemaTable := range schemaMetadata{
		if !schemaTable.Export{
			continue
		}
		_, ok := processedTables[schemaTableName]
		if ok {
			continue
		}
		exportAllTableData(db, writter, schemaMetadata, schemaTable)
	}
}

func printAllTableData(db *sql.DB, writter *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table, processedTables map[string]bool, path []string) map[string]bool {

	_, tableProcessed := processedTables[table.Name]
	currentTable := schemaMetadata[table.Name]
	if tableProcessed || !currentTable.Export {
		return processedTables
	}
	path = append(path, table.Name)
	processedTables[table.Name] = true

	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export{
			continue
		}
		printAllTableData(db, writter, schemaMetadata, tableReference, processedTables, path)

	}

	exportAllTableData(db, writter, schemaMetadata, table)

	for _, reference := range table.ReferencedBy {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export{
			continue
		}
		if !shouldFollowReferenceToLink(path, table, tableReference) {
			continue
		}
		printAllTableData(db, writter, schemaMetadata, tableReference, processedTables, path)

	}
	return processedTables
}

func exportAllTableData (db *sql.DB, writter *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table){
	formattedColumns := strings.Join(table.Columns, ", ")

	sql := fmt.Sprintf(`SELECT %s FROM %s ;`, formattedColumns, table.Name)
	rows := executeQueryWithResults(db, sql)

	for _, row := range rows {
		writter.WriteString(prepareRowInsert(db, table, row, schemaMetadata) + "\n")
	}

}