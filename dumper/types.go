package dumper

type TableKey struct {
	key map[string]string
}

type TableFilter struct {
	TableName string
	Keys      []TableKey
}
