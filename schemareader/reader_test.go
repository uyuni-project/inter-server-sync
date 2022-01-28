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
	expectedIndexes := map[string]UniqueIndex{uniqueIndexName: {Name: uniqueIndexName, Columns: []string{indexCol}}}

	repo := tests.CreateDataRepository()

	// Read Column Names
	repo.ExpectWithRecords(
		`SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position;`,
		sqlmock.NewRows([]string{"column_name"}).
			AddRow(pKCol).
			AddRow(nonIndexCol).
			AddRow(indexCol),
		tableName,
	)

	// Read PK Column Names
	repo.ExpectWithRecords(
		`SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary;`,
		sqlmock.NewRows([]string{"attname"}).AddRow(pKCol),
		tableName,
	)

	// Read PK Sequence
	repo.ExpectWithRecords(
		`WITH sequences AS (
		SELECT sequence_name
			FROM information_schema.sequences
			WHERE sequence_schema = 'public'
		),
		id_constraints AS (
			SELECT
				tc.constraint_name,
				tc.table_name,
				kcu.column_name
			FROM
				information_schema.table_constraints AS tc
				JOIN information_schema.key_column_usage AS kcu
					ON tc.constraint_name = kcu.constraint_name
			WHERE tc.constraint_schema = 'public'
				AND constraint_type = 'PRIMARY KEY'
				AND kcu.ordinal_position = 1
				AND column_name = 'id'
				AND tc.table_name = $1
		)
		SELECT sequence_name
			FROM id_constraints
			JOIN sequences
				ON replace(regexp_replace(constraint_name, '(_id)?_pk(ey)?', ''), '_', '') = replace(regexp_replace(sequence_name, '(_id)?_seq', ''), '_', '')`,
		sqlmock.NewRows([]string{"sequence_name"}),
		tableName,
	)

	// Read Unique Index Names
	repo.ExpectWithRecords(
		`SELECT DISTINCT indexrelid::regclass
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisunique AND NOT i.indisprimary;`,
		sqlmock.NewRows([]string{"indexrelid"}).AddRow(uniqueIndexName),
		tableName,
	)

	// Read Index Columns
	repo.ExpectWithRecords(
		`SELECT DISTINCT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE indexrelid::regclass = $1::regclass;`,
		sqlmock.NewRows([]string{"indexrelid"}).AddRow(indexCol),
		uniqueIndexName,
	)

	// Read Reference Constraint Names
	repo.ExpectWithRecords(
		`SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`,
		sqlmock.NewRows([]string{"constraint_name"}),
		tableName,
	)

	// Read Referenced By Constraint Names
	repo.ExpectWithRecords(
		`SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name = $1;`,
		sqlmock.NewRows([]string{"constraint_name"}),
		tableName,
	)

	table := processTable(repo.DB, tableName, true)

	indexesEqual := reflect.DeepEqual(table.UniqueIndexes, expectedIndexes)
	if !indexesEqual {
		t.Errorf("Error")
	}
}
