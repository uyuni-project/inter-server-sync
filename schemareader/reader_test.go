package schemareader

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/uyuni-project/inter-server-sync/tests"
	"reflect"
	"testing"
)

func TestProcessTable(t *testing.T) {

	tableName := "tableName"
	pKCol := "pKCol"
	uniqueIndexName := "uniqueIndexName"
	indexCol := "uniqueColumn"
	nonIndexCol := "nonUniqueCol"
	expectedIndexes := map[string]UniqueIndex{
		uniqueIndexName: {
			Name:    uniqueIndexName,
			Columns: []string{indexCol},
		},
	}

	repo := tests.CreateDataRepository()
	repo.ExpectWithRecords(
		ReadColumnNames,
		sqlmock.NewRows([]string{"column_name"}).
			AddRow(pKCol).
			AddRow(nonIndexCol).
			AddRow(indexCol),
		tableName,
	)
	repo.ExpectWithRecords(
		ReadPkColumnNames,
		sqlmock.NewRows([]string{"attname"}).
			AddRow(pKCol),
		tableName,
	)
	repo.ExpectWithRecords(
		ReadPkSequence,
		sqlmock.NewRows([]string{"sequence_name"}),
		tableName,
	)
	repo.ExpectWithRecords(
		ReadUniqueIndexNames,
		sqlmock.NewRows([]string{"indexrelid"}).
			AddRow(uniqueIndexName),
		tableName,
	)
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			AddRow(indexCol),
		uniqueIndexName,
	)
	repo.ExpectWithRecords(
		ReadReferenceConstraintNames,
		sqlmock.NewRows([]string{"constraint_name"}),
		tableName,
	)
	repo.ExpectWithRecords(
		ReadReferencedByConstraintNames,
		sqlmock.NewRows([]string{"constraint_name"}),
		tableName,
	)

	table := processTable(repo.DB, tableName, true)

	indexesEqual := reflect.DeepEqual(table.UniqueIndexes, expectedIndexes)
	if !indexesEqual {
		t.Errorf("UniqueIndexes are not expected.")
	}
}
