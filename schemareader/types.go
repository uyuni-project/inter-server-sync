package schemareader

// Table represents a DB table to dump
type Table struct {
	Name          string
	Columns       []string
	PKColumns     map[string]bool
	PKSequence    string
	UniqueIndexes map[string]UniqueIndex
	// a unique index is main when it is the preferred "natural" key
	MainUniqueIndexName string
	References          []Reference
}

// UniqueIndex represents an index among columns of a Table
type UniqueIndex struct {
	Name    string
	Columns []string
}

// Reference represents a foreign key relationship to a Table
type Reference struct {
	TableName     string
	ColumnMapping map[string]string
}
