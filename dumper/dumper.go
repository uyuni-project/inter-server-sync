package dumper

import (
	"database/sql"
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
	"strings"
)

// DumpAllData creates a SQL representation of data in the schema
func DumpAllData(db *sql.DB, tables []schemareader.Table) []string {
	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	result := make([]string, 0)

	for _, table := range tables {
		columnIndexes := make(map[string]int)
		for i, columnName := range table.Columns {
			columnIndexes[columnName] = i
		}
		values := dumpTableValues(db, table, tables)

		for _, value := range values {
			result = append(result, prepareRowInsert(db, table, value, tableMap, columnIndexes))

		}
	}

	return result
}

func dumpTableValues(db *sql.DB, table schemareader.Table, tables []schemareader.Table) [][]rowDataStructure {

	columnNames := strings.Join(table.Columns, ", ")

	sql := fmt.Sprintf(`SELECT %s FROM %s ;`, columnNames, table.Name)
	return executeQueryWithResults(db, sql)
}
