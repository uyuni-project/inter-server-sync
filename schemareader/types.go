package schemareader

// Table represents a DB table to dump
type Table struct {
	Name            string
	Export          bool
	Columns         []string
	UnexportColumns map[string]bool
	ColumnIndexes   map[string]int
	PKColumns       map[string]bool
	PKSequence      string
	UniqueIndexes   map[string]UniqueIndex
	// a unique index is main when it is the preferred "natural" key
	MainUniqueIndexName string
	References          []Reference
	ReferencedBy        []Reference
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

// we are returning just one reference, the first one which uses the column we want
func (table *Table) GetFirstReferenceFromColumn(columnName string) Reference {
	for _, reference := range table.References {
		_, ok := reference.ColumnMapping[columnName]
		if ok {
			return reference
		}
	}
	return Reference{}
}
