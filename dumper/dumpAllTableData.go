package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

func DumpAllTablesData(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
	startingTables []schemareader.Table, whereFilterClause func(table schemareader.Table) string, onlyIfParentExistsTables []string) {

	// exporting from the starting tables.
	processedTables := DumpReachableTablesData(db, writer, schemaMetadata, startingTables, whereFilterClause, onlyIfParentExistsTables, make(map[string]bool))
	// Export tables not visited when exporting the starting tables
	for schemaTableName, schemaTable := range schemaMetadata {
		if !schemaTable.Export {
			continue
		}
		_, ok := processedTables[schemaTableName]
		if ok {
			continue
		}
		exportAllTableData(db, writer, schemaMetadata, schemaTable, whereFilterClause, onlyIfParentExistsTables)
	}
}

func DumpReachableTablesData(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
	startingTables []schemareader.Table, whereFilterClause func(table schemareader.Table) string, onlyIfParentExistsTables []string, processedTables map[string]bool) map[string]bool {

	for _, startingTable := range startingTables {
		_, ok := processedTables[startingTable.Name]
		if ok {
			continue
		}
		processedTables = processTableDataWithLinks(db, writer, schemaMetadata, startingTable, whereFilterClause, processedTables, make([]string, 0), onlyIfParentExistsTables)
	}

	return processedTables
}

func processTableDataWithLinks(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table,
	whereFilterClause func(table schemareader.Table) string, processedTables map[string]bool, path []string, onlyIfParentExistsTables []string) map[string]bool {
	log.Trace().Msgf("Processing table: %s", table.Name)
	_, tableProcessed := processedTables[table.Name]
	currentTable := schemaMetadata[table.Name]
	if tableProcessed || !currentTable.Export {
		return processedTables
	}
	path = append(path, table.Name)
	processedTables[table.Name] = true

	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		log.Trace().Msgf("Table processed: %s", table.Name)
		processTableDataWithLinks(db, writer, schemaMetadata, tableReference, whereFilterClause, processedTables, path, onlyIfParentExistsTables)

	}

	exportAllTableData(db, writer, schemaMetadata, table, whereFilterClause, onlyIfParentExistsTables)

	for _, reference := range table.ReferencedBy {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		if !shouldFollowReferenceToLink(path, table, tableReference) {
			continue
		}
		processTableDataWithLinks(db, writer, schemaMetadata, tableReference, whereFilterClause, processedTables, path, onlyIfParentExistsTables)

	}
	return processedTables
}

func exportAllTableData(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table,
	whereFilterClause func(table schemareader.Table) string, onlyIfParentExistsTables []string) {

	log.Trace().Msgf("Exporting data for table %s", table.Name)
	formattedColumns := strings.Join(table.Columns, ", ")
	sql := fmt.Sprintf(`SELECT %s FROM %s %s;`, formattedColumns, table.Name, whereFilterClause(table))
	rows := sqlUtil.ExecuteQueryWithResults(db, sql)

	for _, row := range rows {
		writer.WriteString(generateRowInsertStatement(db, row, table, schemaMetadata, onlyIfParentExistsTables) + "\n")
	}

}
