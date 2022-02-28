package dumper

import (
	"fmt"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"github.com/uyuni-project/inter-server-sync/tests"
	"reflect"
	"strings"
	"testing"
)

// writerTestCase is a general object for each dumper's recursive method
type writerTestCase struct {
	repo                     *tests.DataRepository
	schemaMetadata           MetaDataGraph
	startingTable            schemareader.Table
	dumper                   DataDumper
	whereFilterClause        func(table schemareader.Table) string
	processedTables          map[string]bool
	path                     []string
	onlyIfParentExistsTables []string
	options                  PrintSqlOptions
}

func TestPrintAllTableData(t *testing.T) {

	// 01 Arrange
	graph := TablesGraph{
		// first order
		"root": []string{"v01", "v02"},
		"v01":  []string{"v05"},
		"v02":  []string{"v03"},
		// second order
		"v03": []string{"v04"},
		// third order with circular dependency
		"v04": []string{"v05"},
		"v05": []string{"v04"},
	}
	root := "root"
	testCase := createTestCase(graph, root, PrintSqlOptions{})

	// the data repository expect these statements in the exact same order
	testCase.repo.Expect("SELECT id, fk_id FROM v04 ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v05 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v05 ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v04 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v01 ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v03 ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v02 ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v03 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM root ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v01 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v02 WHERE id = $1;", 1)

	// 02 Act
	result := processTableDataWithLinks(
		testCase.repo.DB,
		testCase.repo.Writer,
		testCase.schemaMetadata,
		testCase.startingTable,
		testCase.whereFilterClause,
		testCase.processedTables,
		testCase.path,
		testCase.onlyIfParentExistsTables,
	)

	// 03 Assert
	if result == nil {
		t.Errorf("processedTables is nil")
	}
	for node, isExported := range result {
		if !isExported {
			t.Errorf(fmt.Sprintf("Node %v is not exported!", node))
		}
	}
	// checks if all expected statements were indeed executed against the db
	if err := testCase.repo.ExpectationsWereMet(); err != nil {
		t.Errorf("Some nodes left unexported. Error message: %s", err)
	}
}

func TestPrintCleanTables(t *testing.T) {

	// 01 Arrange
	graph := TablesGraph{
		// first order
		"root": []string{"v11", "v12"},
		"v11":  []string{"v15", "v16"},
		"v12":  []string{"v13"},
		// second order
		"v13": []string{"v14"},
		// third order with circular dependency
		"v14": []string{"v15", "v16"},
		"v15": []string{"v14"},
		"v16": []string{},
	}
	keys := make([]string, 0, len(graph))
	for k := range graph {
		keys = append(keys, k)
	}
	root := "root"
	testCase := createTestCase(
		graph,
		root,
		PrintSqlOptions{TablesToClean: keys},
	)

	testCase.repo.Expect("Select * FROM root WHERE (id) IN (SELECT root.id FROM root  );", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v11 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v12 WHERE id = $1;", 1)
	testCase.repo.Expect("Select * FROM v11 WHERE (id) IN (SELECT v11.id FROM v11  "+
		"INNER JOIN root on root.fk_id = v11.id );", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v15 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v16 WHERE id = $1;", 1)
	testCase.repo.Expect("Select * FROM v15 WHERE (id) IN (SELECT v15.id FROM v15  "+
		"INNER JOIN v11 on v11.fk_id = v15.id "+
		"INNER JOIN root on root.fk_id = v11.id );", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v14 WHERE id = $1;", 1)
	testCase.repo.Expect("Select * FROM v14 WHERE (id) IN (SELECT v14.id FROM v14  "+
		"INNER JOIN v15 on v15.fk_id = v14.id "+
		"INNER JOIN v11 on v11.fk_id = v15.id "+
		"INNER JOIN root on root.fk_id = v11.id );", 1)
	testCase.repo.Expect("Select * FROM v16 WHERE (id) IN (SELECT v16.id FROM v16  "+
		"INNER JOIN v14 on v14.fk_id = v16.id "+
		"INNER JOIN v15 on v15.fk_id = v14.id "+
		"INNER JOIN v11 on v11.fk_id = v15.id "+
		"INNER JOIN root on root.fk_id = v11.id );", 1)
	testCase.repo.Expect("Select * FROM v12 WHERE (id) IN (SELECT v12.id FROM v12  "+
		"INNER JOIN root on root.fk_id = v12.id );", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v13 WHERE id = $1;", 1)
	testCase.repo.Expect("Select * FROM v13 WHERE (id) IN (SELECT v13.id FROM v13  "+
		"INNER JOIN v12 on v12.fk_id = v13.id "+
		"INNER JOIN root on root.fk_id = v12.id );", 1)

	expectedWrittenBuffer := []string{
		"" +
			"\n" +
			"DELETE FROM root WHERE (id) IN (SELECT root.id FROM root  );" +
			"\n" +
			"INSERT INTO root (id, fk_id)\t" +
			"select (SELECT id FROM v12 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from root where  id = (SELECT id FROM v12 WHERE id = '0001' limit 1))" +
			" and exists (SELECT id FROM v12 WHERE id = '0001' limit 1);" +
			"\n" +
			"\n" +
			"DELETE FROM v11 WHERE (id) IN (SELECT v11.id FROM v11  INNER JOIN root on root.fk_id = v11.id );" +
			"\n" +
			"INSERT INTO v11 (id, fk_id)\t" +
			"select (SELECT id FROM v16 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from v11 where  id = (SELECT id FROM v16 WHERE id = '0001' limit 1)) " +
			"and exists (SELECT id FROM v16 WHERE id = '0001' limit 1);" +
			"\n" +
			"\n" +
			"DELETE FROM v15 WHERE (id) IN " +
			"(SELECT v15.id FROM v15  INNER JOIN v11 on v11.fk_id = v15.id INNER JOIN root on root.fk_id = v11.id );" +
			"\n" +
			"INSERT INTO v15 (id, fk_id)\t" +
			"select (SELECT id FROM v14 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from v15 where  id = (SELECT id FROM v14 WHERE id = '0001' limit 1)) " +
			"and exists (SELECT id FROM v14 WHERE id = '0001' limit 1);" +
			"\n" +
			"\n" +
			"DELETE FROM v14 WHERE (id) IN " +
			"(SELECT v14.id FROM v14  " +
			"INNER JOIN v15 on v15.fk_id = v14.id " +
			"INNER JOIN v11 on v11.fk_id = v15.id " +
			"INNER JOIN root on root.fk_id = v11.id );" +
			"\n" +
			"INSERT INTO v14 (id, fk_id)\t" +
			"select (SELECT id FROM v16 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from v14 where  id = (SELECT id FROM v16 WHERE id = '0001' limit 1)) " +
			"and exists (SELECT id FROM v16 WHERE id = '0001' limit 1);" +
			"\n" +
			"\n" +
			"DELETE FROM v16 WHERE (id) IN " +
			"(SELECT v16.id FROM v16  " +
			"INNER JOIN v14 on v14.fk_id = v16.id " +
			"INNER JOIN v15 on v15.fk_id = v14.id " +
			"INNER JOIN v11 on v11.fk_id = v15.id " +
			"INNER JOIN root on root.fk_id = v11.id );" +
			"\n" +
			"INSERT INTO v16 (id, fk_id)\t" +
			"select '0001','0001'  " +
			"where  not exists (select 1 from v16 where  id = '0001') " +
			"and ;" +
			"\n" +
			"\n" +
			"DELETE FROM v12 WHERE (id) IN " +
			"(SELECT v12.id FROM v12  " +
			"INNER JOIN root on root.fk_id = v12.id );" +
			"\n" +
			"INSERT INTO v12 (id, fk_id)\t" +
			"select (SELECT id FROM v13 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from v12 where  id = (SELECT id FROM v13 WHERE id = '0001' limit 1)) " +
			"and exists (SELECT id FROM v13 WHERE id = '0001' limit 1);" +
			"\n" +
			"\n" +
			"DELETE FROM v13 WHERE (id) IN " +
			"(SELECT v13.id FROM v13  " +
			"INNER JOIN v12 on v12.fk_id = v13.id " +
			"INNER JOIN root on root.fk_id = v12.id );" +
			"\n" +
			"INSERT INTO v13 (id, fk_id)\t" +
			"select (SELECT id FROM v14 WHERE id = '0001' limit 1),'0001'  " +
			"where  not exists (select 1 from v13 where  id = (SELECT id FROM v14 WHERE id = '0001' limit 1)) " +
			"and exists (SELECT id FROM v14 WHERE id = '0001' limit 1);" +
			"\n",
	}

	// 02 Act
	printCleanTables(
		testCase.repo.DB,
		testCase.repo.Writer,
		testCase.schemaMetadata,
		testCase.startingTable,
		testCase.processedTables,
		testCase.path,
		testCase.options,
	)
	writtenBuffer := testCase.repo.GetWriterBuffer()

	// 03 Assert
	if testCase.processedTables == nil {
		t.Errorf("processedTables is nil")
	}

	buffersEqual := reflect.DeepEqual(writtenBuffer, expectedWrittenBuffer)
	if !buffersEqual {
		t.Errorf("Buffers are not equal")
	}

	for node, isExported := range testCase.processedTables {
		if !isExported {
			t.Errorf(fmt.Sprintf("Node %v is not exported!", node))
		}
	}
	// checks if all expected statements were indeed executed against the db
	if err := testCase.repo.ExpectationsWereMet(); err != nil {
		t.Errorf("Some nodes left unexported. Error message: %s", err)
	}
}

func TestPrintTableData(t *testing.T) {

	// 01 Arrange
	graph := TablesGraph{
		// first order
		"root": []string{"v21", "v22"},
		"v21":  []string{"v25", "v26"},
		"v22":  []string{"v23"},
		// second order
		"v23": []string{"v24"},
		// third order with circular dependency
		"v24": []string{"v25", "v26"},
		"v25": []string{"v24"},
		"v26": []string{},
	}
	root := "root"
	testCase := createTestCase(graph, root, PrintSqlOptions{PostOrderCallback: createCallback()})

	// the data repository expect these statements in the exact same order
	testCase.repo.Expect("SELECT id, fk_id FROM v26 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v24 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v25 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v26 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v25 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v24 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v21 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v23 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v22 WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v23 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM root WHERE (id) in (('0001'));", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v21 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v22 WHERE id = $1;", 1)

	// 02 Act
	printTableData(
		testCase.repo.DB,
		testCase.repo.Writer,
		testCase.schemaMetadata,
		testCase.dumper,
		testCase.startingTable,
		testCase.processedTables,
		testCase.path,
		testCase.options,
	)

	// 03 Assert
	if testCase.processedTables == nil {
		t.Errorf("processedTables is nil")
	}
	for node, isExported := range testCase.processedTables {
		if !isExported {
			t.Errorf(fmt.Sprintf("Node %v is not exported!", node))
		}
	}
	// checks if all expected statements were indeed executed against the db
	if err := testCase.repo.ExpectationsWereMet(); err != nil {
		t.Errorf("Some nodes left unexported. Error message: %s", err)
	}
}

func TestFormatOnConflict(t *testing.T) {
	// 01 Arrange
	row := []sqlUtil.RowDataStructure{
		{ColumnName: "org_id", Value: TableDump{}},
	}
	table := schemareader.Table{Name: "rhnerrata"}
	expectedResult := "(advisory, org_id) WHERE org_id IS NOT NULL DO UPDATE SET "

	// 02 Act
	result := formatOnConflict(row, table)

	// 03 Assert
	if strings.Compare(result, expectedResult) != 0 {
		t.Errorf(fmt.Sprintf("Expected %s, but got %s", expectedResult, result))
	}
}

func TestFormatOnConflictRhnConfigInfo(t *testing.T) {
	// 01 Arrange
	table := schemareader.Table{Name: "rhnconfiginfo"}

	type Case struct {
		row        []sqlUtil.RowDataStructure
		constraint string
	}
	testCases := []Case{
		{
			row: []sqlUtil.RowDataStructure{
				{ColumnName: "username", Value: nil},
				{ColumnName: "groupname", Value: nil},
				{ColumnName: "filemode", Value: nil},
				{ColumnName: "selinux_ctx", Value: TableDump{}},
				{ColumnName: "symlink_target_filename_id", Value: TableDump{}},
			},
			// rhn_confinfo_s_se_uq
			constraint: "(symlink_target_filename_id, selinux_ctx) " +
				"WHERE username IS NULL " +
				"AND groupname IS NULL " +
				"AND filemode IS NULL " +
				"AND selinux_ctx IS NOT NULL " +
				"AND symlink_target_filename_id IS NOT NULL",
		},
		{
			row: []sqlUtil.RowDataStructure{
				{ColumnName: "username", Value: nil},
				{ColumnName: "groupname", Value: nil},
				{ColumnName: "filemode", Value: nil},
				{ColumnName: "selinux_ctx", Value: nil},
				{ColumnName: "symlink_target_filename_id", Value: TableDump{}},
			},
			// rhn_confinfo_s_uq
			constraint: "(symlink_target_filename_id) " +
				"WHERE username IS NULL " +
				"AND groupname IS NULL " +
				"AND filemode IS NULL " +
				"AND selinux_ctx IS NULL " +
				"AND symlink_target_filename_id IS NOT NULL",
		},
		{
			row: []sqlUtil.RowDataStructure{
				{ColumnName: "username", Value: TableDump{}},
				{ColumnName: "groupname", Value: TableDump{}},
				{ColumnName: "filemode", Value: TableDump{}},
				{ColumnName: "selinux_ctx", Value: TableDump{}},
				{ColumnName: "symlink_target_filename_id", Value: nil},
			},
			// rhn_confinfo_ugf_se_uq
			constraint: "(username, groupname, filemode, selinux_ctx) " +
				"WHERE username IS NOT NULL " +
				"AND groupname IS NOT NULL " +
				"AND filemode IS NOT NULL " +
				"AND selinux_ctx IS NOT NULL " +
				"AND symlink_target_filename_id IS NULL",
		},
		{
			row: []sqlUtil.RowDataStructure{
				{ColumnName: "username", Value: TableDump{}},
				{ColumnName: "groupname", Value: TableDump{}},
				{ColumnName: "filemode", Value: TableDump{}},
				{ColumnName: "selinux_ctx", Value: nil},
				{ColumnName: "symlink_target_filename_id", Value: nil},
			},
			// rhn_confinfo_ugf_uq
			constraint: "(username, groupname, filemode) " +
				"WHERE username IS NOT NULL " +
				"AND groupname IS NOT NULL " +
				"AND filemode IS NOT NULL " +
				"AND selinux_ctx IS NULL " +
				"AND symlink_target_filename_id IS NULL",
		},
	}

	for i, c := range testCases {
		// 02 Act
		result := formatOnConflict(c.row, table)

		// 03 Assert
		expected := c.constraint + " DO UPDATE SET "
		if strings.Compare(result, expected) != 0 {
			t.Errorf(fmt.Sprintf("Case # %d: expected %s, but got %s", i, expected, result))
		}
	}
}

// createTestCase is a factory method for writerTestCase
func createTestCase(graph TablesGraph, root string, options PrintSqlOptions) writerTestCase {
	repo := tests.CreateDataRepository()
	tablesMetaData, dataDumper := initializeMetaDataGraph(graph, root)
	return writerTestCase{
		repo,
		tablesMetaData,
		tablesMetaData[root],
		dataDumper,
		func(table schemareader.Table) string { return "" },
		map[string]bool{},
		[]string{},
		[]string{},
		options,
	}
}
