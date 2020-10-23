package dumper

type TableKey struct {
	key map[string]string
}

type TableDump struct {
	TableName string
	KeyMap    map[string]bool
	Keys      []TableKey
	Queries   []string
}

type DataDumper struct {
	TableData map[string]TableDump
}
