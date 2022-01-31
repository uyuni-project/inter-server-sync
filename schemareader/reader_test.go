package schemareader

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/uyuni-project/inter-server-sync/tests"
	"reflect"
	"testing"
)

const (
	TableName          = "TableName"
	PKColumnName       = "PKColumnName"
	UniqueIndexName01  = "UniqueIndexName01"
	UniqueIndexName02  = "UniqueIndexName02"
	IndexColumnName    = "IndexColumnName"
	NonIndexColumnName = "NonIndexColumnName"
)

func TestProcessTable(t *testing.T) {

	// Arrange
	repo := tests.CreateDataRepository()
	UniqueIndexMostColumnsCase(repo)

	// Act
	table := processTable(repo.DB, TableName, true)

	// Assert
	indexesEqual := reflect.DeepEqual(table.MainUniqueIndexName, UniqueIndexName02)
	if !indexesEqual {
		t.Errorf("UniqueIndexes are not expected.")
	}
}

func UniqueIndexMostColumnsCase(repo *tests.DataRepository) {
	repo.ExpectWithRecords(
		ReadColumnNames,
		sqlmock.NewRows([]string{"column_name"}).
			AddRow(PKColumnName).
			AddRow(NonIndexColumnName).
			AddRow(IndexColumnName),
		TableName,
	)
	repo.ExpectWithRecords(
		ReadPkColumnNames,
		sqlmock.NewRows([]string{"attname"}).
			AddRow(PKColumnName),
		TableName,
	)
	repo.ExpectWithRecords(
		ReadPkSequence,
		sqlmock.NewRows([]string{"sequence_name"}),
		TableName,
	)

	// Read indexes information
	repo.ExpectWithRecords(
		ReadUniqueIndexNames,
		sqlmock.NewRows([]string{"indexrelid"}).
			AddRow(UniqueIndexName01).
			AddRow(UniqueIndexName02),
		TableName,
	)
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			// One column in the index
			AddRow(PKColumnName),
		UniqueIndexName01,
	)
	repo.ExpectWithRecords(
		ReadIndexColumns,
		sqlmock.NewRows([]string{"indexrelid"}).
			// Two columns in the index
			AddRow(PKColumnName).
			AddRow(IndexColumnName),
		UniqueIndexName02,
	)

	repo.ExpectWithRecords(
		ReadReferenceConstraintNames,
		sqlmock.NewRows([]string{"constraint_name"}),
		TableName,
	)
	repo.ExpectWithRecords(
		ReadReferencedByConstraintNames,
		sqlmock.NewRows([]string{"constraint_name"}),
		TableName,
	)
}
