package schemareader

// Table represents a DB table to dump
type Table struct {
	Name          string
	Columns       []string
	PKColumns     map[string]bool
	UniqueIndexes []UniqueIndex
}

// UniqueIndex represents an index among columns of a Table
type UniqueIndex struct {
	Name    string
	Columns []string
}
