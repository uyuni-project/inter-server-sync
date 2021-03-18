package dumper

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
	row       []rowDataStructure
	path      []string
}

type rowDataStructure struct {
	columnName   string
	columnType   string
	initialValue interface{}
	value        interface{}
}
