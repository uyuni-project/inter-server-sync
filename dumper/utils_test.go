package dumper

import (
	"fmt"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"strings"
)

type TablesGraph map[string][]string
type MetaDataGraph map[string]schemareader.Table

// initializeMetaDataGraph creates MetaDataGraph and DataDumper in two separate routines, that traverse the TablesGraph
// from the given root in different orders to get the desired setup
func initializeMetaDataGraph(graph TablesGraph, root string) (MetaDataGraph, DataDumper) {

	schemaMetadata, dataDumper := metaDataLevelOrder(graph, root)
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

func allPathsPostOrder(graph TablesGraph, root string) map[string]bool {

	result := map[string]bool{}

	visited := map[string]bool{}
	var node string

	// Two stacks approach
	stack := []string{root}
	var path []string

	for len(stack) > 0 {
		// pop the next node from the stack
		node, stack = stack[len(stack)-1], stack[:len(stack)-1]

		// update the current path if we're done with all paths from a branch
		if len(stack) == 0 && node != root {
			path = []string{root}
		}

		// there are circular dependencies, so we need to check if we've been there yet
		if _, ok := visited[node]; ok {
			continue
		}

		// process the current node
		visited[node] = true
		path = append(path, node)
		stack = append(stack, graph[node]...)

		result[strings.Join(path, ",")] = true

		// pop from the path stack if we've reached a leaf
		if len(graph[node]) == 0 {
			path = path[:len(path)-1]
		}
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
