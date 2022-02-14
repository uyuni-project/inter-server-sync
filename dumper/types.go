package dumper

import (
	"bufio"
	"database/sql"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

type RowKey struct {
	Column string
	Value  string
}

type TableKey struct {
	Key []RowKey
}

type TableDump struct {
	TableName string
	KeyMap    map[string]bool
	Keys      []TableKey
}

type DataDumper struct {
	TableData map[string]TableDump
	Paths     map[string]bool
}

type processItem struct {
	tableName string
	row       []sqlUtil.RowDataStructure
	path      []string
}

type PrintSqlOptions struct {
	TablesToClean            []string
	CleanWhereClause         string
	OnlyIfParentExistsTables []string
	PostOrderCallback        Callback
}

type Callback func(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table, data DataDumper)
