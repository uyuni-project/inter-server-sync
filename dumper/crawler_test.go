package dumper

import (
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/tests"
	"reflect"
	"testing"
)

// crawlerTestCase lays down a test scenario for the DataCrawler func
type crawlerTestCase struct {
	repo               *tests.DataRepository
	schemaMetadata     MetaDataGraph
	startTable         schemareader.Table
	startQueryFilter   string
	expectedDataDumper DataDumper
}

func TestShouldCreateDataDumper(t *testing.T) {

	// Arrange
	graph := TablesGraph{
		// first order
		"root": []string{"v31", "v32"},
		"v31":  []string{"v35", "v36"},
		"v32":  []string{"v33"},
		// second order
		"v33": []string{"v34"},
		// third order with circular dependency
		"v34": []string{"v35", "v36"},
		"v35": []string{"v34"},
		"v36": []string{},
	}
	root := "root"
	testCase := createDataCrawlerTestCase(graph, root)

	// the data repository expect these statements in the exact same order
	testCase.repo.Expect("SELECT * FROM root where CUSTOM ;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v31 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v32 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v33 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v34 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v35 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v36 WHERE id = $1;", 1)

	testCase.repo.Expect("SELECT id, fk_id FROM v35 WHERE id = $1;", 1)
	testCase.repo.Expect("SELECT id, fk_id FROM v36 WHERE id = $1;", 1)

	// Act
	dataDumper := DataCrawler(
		testCase.repo.DB,
		testCase.schemaMetadata,
		testCase.startTable,
		testCase.startQueryFilter,
	)

	// Assert
	if dataDumper.TableData == nil || dataDumper.Paths == nil {
		t.Errorf("DataDumper was not initiated")
	}
	tableDataEqual := reflect.DeepEqual(dataDumper.TableData, testCase.expectedDataDumper.TableData)
	if !tableDataEqual {
		t.Errorf("DataDumper.TableData is not expected")
	}
	pathsEqual := reflect.DeepEqual(dataDumper.Paths, testCase.expectedDataDumper.Paths)
	if !pathsEqual {
		t.Errorf("DataDumper.Paths is not expected")
	}
}

// createTestCase is a factory method for writerTestCase
func createDataCrawlerTestCase(graph TablesGraph, root string) crawlerTestCase {
	repo := tests.CreateDataRepository()
	tablesMetaData, dataDumper := initializeMetaDataGraph(graph, root)
	return crawlerTestCase{
		repo:               repo,
		schemaMetadata:     tablesMetaData,
		startTable:         tablesMetaData[root],
		startQueryFilter:   "CUSTOM",
		expectedDataDumper: dataDumper,
	}
}

// followLinkTestCase lays down a test scenario for the shouldFollowReferenceToLink func
type followLinkTestCase struct {
	path            []string // path constructed by a recursive function so far
	currentTable    schemareader.Table
	referencedTable schemareader.Table
}

func TestShouldFollowForcedNavigations(t *testing.T) {

	// Arrange
	var shouldFollow bool
	testCase := followLinkTestCase{
		path: []string{},
		currentTable: schemareader.Table{
			Name: "rhnchannel",
		},
		referencedTable: schemareader.Table{
			Name: "susemddata",
		},
	}

	// Act
	shouldFollow = shouldFollowReferenceToLink(
		testCase.path,
		testCase.currentTable,
		testCase.referencedTable,
	)

	// Assert
	if !shouldFollow {
		t.Errorf("Should follow along forcedNavigations to the referencedTable")
	}
}

func TestShouldNotFollowInPath(t *testing.T) {

	// Arrange
	var shouldFollow bool
	testCase := followLinkTestCase{
		path: []string{"target"},
		currentTable: schemareader.Table{
			Name: "source",
		},
		referencedTable: schemareader.Table{
			Name: "target",
		},
	}

	// Act
	shouldFollow = shouldFollowReferenceToLink(
		testCase.path,
		testCase.currentTable,
		testCase.referencedTable,
	)

	// Assert
	if shouldFollow {
		t.Errorf("Should not follow the referencedTable if it is already in the path")
	}
}

func TestShouldFollowLinkingTable(t *testing.T) {

	// Arrange
	var shouldFollow bool
	testCase := followLinkTestCase{
		path: []string{},
		currentTable: schemareader.Table{
			Name: "source",
		},
		referencedTable: schemareader.Table{
			Name:       "sourcetarget",
			References: []schemareader.Reference{{TableName: "targetreference"}},
		},
	}

	// Act
	shouldFollow = shouldFollowReferenceToLink(
		testCase.path,
		testCase.currentTable,
		testCase.referencedTable,
	)

	// Assert
	if !shouldFollow {
		t.Errorf("Should follow the referencedTable if it is a linking table and not referenced by others")
	}
}

func TestShouldNotFollowLinkingTable(t *testing.T) {

	// Arrange
	var shouldFollow bool
	testCase := followLinkTestCase{
		path: []string{"targetreference"},
		currentTable: schemareader.Table{
			Name: "source",
		},
		referencedTable: schemareader.Table{
			Name:       "sourcetarget",
			References: []schemareader.Reference{{TableName: "targetreference"}},
		},
	}

	// Act
	shouldFollow = shouldFollowReferenceToLink(
		testCase.path,
		testCase.currentTable,
		testCase.referencedTable,
	)

	// Assert
	if shouldFollow {
		t.Errorf("Should not follow the referencedTable if one of the tables it itself references is already in the path")
	}
}

func TestShouldNotFollowReferencedLinkingTable(t *testing.T) {

	// Arrange
	var shouldFollow bool
	testCase := followLinkTestCase{
		path: []string{},
		currentTable: schemareader.Table{
			Name: "source",
		},
		referencedTable: schemareader.Table{
			Name:         "sourcetarget",
			ReferencedBy: []schemareader.Reference{{TableName: "other"}},
		},
	}

	// Act
	shouldFollow = shouldFollowReferenceToLink(
		testCase.path,
		testCase.currentTable,
		testCase.referencedTable,
	)

	// Assert
	if shouldFollow {
		t.Errorf("Should not follow the referencedTable if it is a linking table but also is referenced by others")
	}
}
