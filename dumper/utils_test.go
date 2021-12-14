package dumper

import (
	"fmt"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"reflect"
	"strings"
)

type TablesGraph map[string][]string
type MetaDataGraph map[string]schemareader.Table

// initializeMetaDataGraph creates MetaDataGraph and DataDumper in two separate routines, that traverse the TablesGraph
// from the given root in different orders to get the desired setup
func initializeMetaDataGraph(graph TablesGraph, root string) (MetaDataGraph, DataDumper) {

	schemaMetadata, dataDumper := metaDataLevelOrderV2(graph, root)
	dataDumper.Paths = allPathsPostOrder(graph, root)
	return schemaMetadata, dataDumper
}

// Breadth-First-Search
// Iteratively creates a MetaDataGraph map together with a DataDumper object according to a TablesGraph specification
func metaDataLevelOrder(graph TablesGraph, root string) (MetaDataGraph, DataDumper) {

	schemaMetadata := MetaDataGraph{}
	dataDumper := DataDumper{
		TableData: map[string]TableDump{},
		Paths:     map[string]bool{},
	}

	node := root
	queue := []string{root}
	parentNode := ""
	levelSize := len(queue)
	for len(queue) > 0 {
		// Dequeue, check if visited and decrement the level size
		node, queue = queue[0], queue[1:]
		if _, ok := schemaMetadata[node]; ok {
			continue
		}
		levelSize -= 1

		// create a table and add a referer if there is any
		indexName := schemareader.VirtualIndexName
		table := schemareader.Table{
			Name:                node,
			Export:              true,
			Columns:             []string{"id", "fk_id"},
			PKColumns:           map[string]bool{"id": true},
			MainUniqueIndexName: indexName,
			UniqueIndexes:       map[string]schemareader.UniqueIndex{indexName: {indexName, []string{"id"}}},
		}
		if len(parentNode) > 0 {
			table.ReferencedBy = []schemareader.Reference{{parentNode, map[string]string{"fk_id": "id"}}}
		}

		// add referenced tables to the node
		table.References = []schemareader.Reference{}
		for _, ref := range graph[node] {
			table.References = append(
				table.References,
				schemareader.Reference{TableName: ref, ColumnMapping: map[string]string{"fk_id": "id"}},
			)
		}

		// create a record in MetaDataGraph and TableDump
		schemaMetadata[node] = table
		dataDumper.TableData[node] = TableDump{
			TableName: node,
			KeyMap:    map[string]bool{fmt.Sprintf("'%04d'", 1): true},
			Keys:      []TableKey{{Key: map[string]string{"id": fmt.Sprintf("'%04d'", 1)}}},
		}
		dataDumper.Paths[node] = true

		// move forward, check if we're done with a level, update the parent if so
		queue = append(queue, graph[node]...)
		if levelSize == 0 {
			levelSize += len(graph[node])
			parentNode = node
		}
	}
	return schemaMetadata, dataDumper
}

func metaDataLevelOrderV2(graph TablesGraph, root string) (MetaDataGraph, DataDumper) {
	schemaMetadata := MetaDataGraph{}
	dataDumper := DataDumper{
		TableData: map[string]TableDump{},
		Paths:     map[string]bool{},
	}
	for parent, children := range graph {
		var table schemareader.Table
		if _, ok := schemaMetadata[parent]; !ok {
			// create a table and add a referer if there is any
			indexName := schemareader.VirtualIndexName
			table = schemareader.Table{
				Name:                parent,
				Export:              true,
				Columns:             []string{"id", "fk_id"},
				PKColumns:           map[string]bool{"id": true},
				MainUniqueIndexName: indexName,
				UniqueIndexes:       map[string]schemareader.UniqueIndex{indexName: {indexName, []string{"id"}}},
				References:          []schemareader.Reference{},
				ReferencedBy:        []schemareader.Reference{},
			}
		} else {
			table = schemaMetadata[parent]
		}
		for _, child := range children {
			table.References = append(
				table.References,
				schemareader.Reference{TableName: child, ColumnMapping: map[string]string{"fk_id": "id"}},
			)
			var childTable schemareader.Table
			if _, ok := schemaMetadata[child]; !ok {
				indexName := schemareader.VirtualIndexName
				childTable = schemareader.Table{
					Name:                child,
					Export:              true,
					Columns:             []string{"id", "fk_id"},
					PKColumns:           map[string]bool{"id": true},
					MainUniqueIndexName: indexName,
					UniqueIndexes:       map[string]schemareader.UniqueIndex{indexName: {indexName, []string{"id"}}},
					References:          []schemareader.Reference{},
					ReferencedBy:        []schemareader.Reference{},
				}
			} else {
				childTable = schemaMetadata[child]
			}
			childTable.ReferencedBy = append(
				childTable.ReferencedBy,
				schemareader.Reference{TableName: parent, ColumnMapping: map[string]string{"fk_id": "id"}},
			)
			schemaMetadata[child] = childTable
			dataDumper.TableData[child] = TableDump{
				TableName: child,
				KeyMap:    map[string]bool{fmt.Sprintf("'%04d'", 1): true},
				Keys:      []TableKey{{Key: map[string]string{"id": fmt.Sprintf("'%04d'", 1)}}},
			}

		}
		schemaMetadata[parent] = table
		dataDumper.TableData[parent] = TableDump{
			TableName: parent,
			KeyMap:    map[string]bool{fmt.Sprintf("'%04d'", 1): true},
			Keys:      []TableKey{{Key: map[string]string{"id": fmt.Sprintf("'%04d'", 1)}}},
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
		keys = append(keys, TableKey{Key: map[string]string{"id": fmt.Sprintf("%04d", i+1)}})
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
