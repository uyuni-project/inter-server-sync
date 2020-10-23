package dumper

type TableKey struct {
	key map[string]string
}

type TableDump struct {
	TableName string
	Keys      map[string]TableKey
	Queries   []string
}

type DataDumper struct {
	TableData map[string]TableDump
}
