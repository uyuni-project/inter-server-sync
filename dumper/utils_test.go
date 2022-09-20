package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

type TablesGraph map[string][]string
type MetaDataGraph map[string]schemareader.Table

// initializeMetaDataGraph creates MetaDataGraph and DataDumper in two separate routines, that traverse the TablesGraph
// from the given root in different orders to get the desired setup
func initializeMetaDataGraph(graph TablesGraph, root string) (MetaDataGraph, DataDumper) {

	schemaMetadata, dataDumper := createMetaDataGraph(graph)
	dataDumper.Paths = allPathsPostOrder(graph, root)
	return schemaMetadata, dataDumper
}

// createMetaDataGraph iterates over each key in the map, then over each value under this key, creates a table in the
// MetaDataGraph if it does not exist yet, otherwise updates.
func createMetaDataGraph(graph TablesGraph) (MetaDataGraph, DataDumper) {
	schemaMetadata := MetaDataGraph{}
	dataDumper := DataDumper{
		TableData: map[string]TableDump{},
		Paths:     map[string]bool{},
	}
	var getOrCreateTable = func(name string) schemareader.Table {
		if _, ok := schemaMetadata[name]; !ok {
			// create a table and add a referer if there is any
			indexName := schemareader.VirtualIndexName
			return schemareader.Table{
				Name:                name,
				Export:              true,
				Columns:             []string{"id"},
				PKColumns:           map[string]bool{"id": true},
				ColumnIndexes:       map[string]int{"id": 0},
				MainUniqueIndexName: indexName,
				UniqueIndexes:       map[string]schemareader.UniqueIndex{indexName: {indexName, []string{"id"}}},
				References:          []schemareader.Reference{},
				ReferencedBy:        []schemareader.Reference{},
			}
		} else {
			return schemaMetadata[name]
		}
	}
	for parent, children := range graph {
		var parentTable = getOrCreateTable(parent)
		for _, child := range children {
			columnKey := child + "_fk_id"
			parentTable.Columns = append(parentTable.Columns, columnKey)
			parentTable.ColumnIndexes[columnKey] = len(parentTable.Columns) - 1
			parentTable.References = append(
				parentTable.References,
				schemareader.Reference{TableName: child, ColumnMapping: map[string]string{columnKey: "id"}},
			)

			childTable := getOrCreateTable(child)
			childTable.ReferencedBy = append(
				childTable.ReferencedBy,
				schemareader.Reference{TableName: parent, ColumnMapping: map[string]string{columnKey: "id"}},
			)
			schemaMetadata[child] = childTable
			k := []RowKey{{"id", fmt.Sprintf("'%04d'", 1)}}
			dataDumper.TableData[child] = TableDump{
				TableName: child,
				KeyMap:    map[string]bool{fmt.Sprintf("'%04d'", 1): true},
				Keys:      []TableKey{{Key: k}},
			}
		}
		schemaMetadata[parent] = parentTable
		k := []RowKey{{"id", fmt.Sprintf("'%04d'", 1)}}
		dataDumper.TableData[parent] = TableDump{
			TableName: parent,
			KeyMap:    map[string]bool{fmt.Sprintf("'%04d'", 1): true},
			Keys:      []TableKey{{Key: k}},
		}
	}
	return schemaMetadata, dataDumper
}

func allPathsPostOrder(graph TablesGraph, root string) map[string]bool {

	var node string
	var path []string
	stack := []string{root}
	visited := map[string]bool{}
	result := map[string]bool{}
	for len(stack) > 0 {
		// pop the next node from the stack
		node, stack = stack[0], stack[1:]

		// there are circular dependencies, so we need to check if we've been there yet
		if _, ok := visited[node]; ok {
			// rewind from the current depth
			path = path[:len(stack)]
			continue
		}

		visited[node] = true
		path = append(path, node)
		result[strings.Join(path, ",")] = true

		children := graph[node]

		// if reached a leaf
		if len(children) == 0 {
			// make one step back
			path = path[:len(path)-1]
		}

		reverse(children)
		stack = append(children, stack...)

	}
	return result
}

// setNumberOfRecordsForTable takes a generic mocked DataDumper object and simulates a case where a table X has Y records
func setNumberOfRecordsForTable(tc *writerTestCase, tableName string, num int) {
	var keys []TableKey
	for i := 0; i < num; i++ {
		k := []RowKey{{"id", fmt.Sprintf("%04d", i+1)}}
		keys = append(keys, TableKey{Key: k})
	}
	tableData := tc.dumper.TableData[tableName]
	tableData.Keys = keys
	tc.dumper.TableData[tableName] = tableData
}

func reverse(s interface{}) {
	n := reflect.ValueOf(s).Len()
	swap := reflect.Swapper(s)
	for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}
}

func createCallback() Callback {
	return func(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table, data DataDumper) {
	}
}
