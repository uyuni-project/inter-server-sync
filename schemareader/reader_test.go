package schemareader

import (
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/uyuni-project/inter-server-sync/tests"
)

const (
	TableName    = "TableName"
	PKColumnName = "PKColumnName"

	UniqueIndexName01 = "UniqueIndexName01"
	UniqueIndexName02 = "UniqueIndexName02"
	UniqueIndexName03 = "UniqueIndexName03"

	IndexColumnName01 = "IndexColumnName01"
	IndexColumnName02 = "IndexColumnName02"
)

func TestProcessTable(t *testing.T) {

	// Arrange
	repo := tests.CreateDataRepository()
	UniqueIndexMostColumnsCase(repo)

	// Act
	table, _ := processTable(repo.DB, TableName, true)

	// Assert
	indexesEqual := reflect.DeepEqual(table.MainUniqueIndexName, UniqueIndexName03)
	if !indexesEqual {
		t.Errorf("UniqueIndexes do not match: expected %s, got %s", UniqueIndexName03, table.MainUniqueIndexName)
	}
}

func UniqueIndexMostColumnsCase(repo *tests.DataRepository) {

	repo.ExpectWithRecords(ReadColumnNames, sqlmock.NewRows([]string{"column_name"}).AddRow(""), TableName)
	repo.ExpectWithRecords(ReadPkColumnNames, sqlmock.NewRows([]string{"attname"}).AddRow(""), TableName)
	repo.ExpectWithRecords(ReadPkSequence, sqlmock.NewRows([]string{"sequence_name"}).AddRow(""), TableName)

	// Read indexes information to get three indexes
	repo.ExpectWithRecords(
		ReadUniqueIndexNames,
		sqlmock.NewRows([]string{"indexrelid"}).
			AddRow(UniqueIndexName01).
			AddRow(UniqueIndexName02).
			AddRow(UniqueIndexName03),
		TableName,
	)
	// Read columns for index UniqueIndexName01
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			// One column in the index
			AddRow(PKColumnName),
		UniqueIndexName01,
	)
	// Read columns for index UniqueIndexName02
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			// Two columns in the index
			AddRow(PKColumnName).
			AddRow(IndexColumnName01),
		UniqueIndexName02,
	)
	// Read columns for index UniqueIndexName03
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			// Three columns in the index
			AddRow(PKColumnName).
			AddRow(IndexColumnName01).
			AddRow(IndexColumnName02),
		UniqueIndexName03,
	)

	repo.ExpectWithRecords(ReadReferenceConstraintNames, sqlmock.NewRows([]string{"constraint_name"}), TableName)
	repo.ExpectWithRecords(ReadReferencedByConstraintNames, sqlmock.NewRows([]string{"constraint_name"}), TableName)
}
