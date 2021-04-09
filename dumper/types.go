package dumper

import "github.com/uyuni-project/inter-server-sync/sqlUtil"

type TableKey struct {
	Key map[string]interface{}
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
	OnlyIfParentExistsTables [] string
}

