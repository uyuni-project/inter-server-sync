// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package schemareader

import (
	"database/sql"
	"strings"

	"github.com/rs/zerolog/log"
)

func readTableNames(db *sql.DB) []string {
	sql := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_type = 'BASE TABLE';`

	rows, err := db.Query(sql)
	if err != nil {
		log.Panic().Err(err).Msg("error executing database query")
	}

	result := make([]string, 0)
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			log.Panic().Err(err).Msg("error extracting row")
		}
		result = append(result, tableName)
	}

	return result
}

func readColumnNames(db *sql.DB, tableName string) []string {
	sql := `SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error accessing the database")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Panic().Err(err).Msg("error extracting row")
		}
		result = append(result, columnName)
	}

	return result
}

func readPKColumnNames(db *sql.DB, tableName string) []string {
	// https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
	sql := `SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Panic().Err(err).Msg("error getting row data")
		}
		result = append(result, columnName)
	}

	return result
}

func readUniqueIndexNames(db *sql.DB, tableName string) []string {
	sql := `SELECT DISTINCT indexrelid::regclass
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisunique AND NOT i.indisprimary;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Panic().Err(err).Msg("error getting column data")
		}
		result = append(result, name)
	}

	return result
}

func readIndexColumns(db *sql.DB, indexName string) []string {
	sql := `SELECT DISTINCT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE indexrelid::regclass = $1::regclass;`

	rows, err := db.Query(sql, indexName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Panic().Err(err).Msg("error getting column data")
		}
		result = append(result, name)
	}

	return result
}

func readReferenceConstraintNames(db *sql.DB, tableName string) []string {
	sql := `SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Panic().Err(err).Msg("error getting column data")
		}
		result = append(result, name)
	}

	return result
}

func readReferencedByConstraintNames(db *sql.DB, tableName string) []string {
	sql := `SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name = $1;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Panic().Err(err).Msg("error getting column data")
		}
		result = append(result, name)
	}

	return result
}

func readReferencedTable(db *sql.DB, referenceConstraintName string) string {
	sql := `SELECT DISTINCT ccu.table_name
	FROM information_schema.constraint_column_usage AS ccu
	WHERE ccu.constraint_name = $1;`

	rows, err := db.Query(sql, referenceConstraintName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	var name string
	rows.Next()
	rows.Scan(&name)

	return name
}

func readReferencedByTable(db *sql.DB, referenceConstraintName string) string {
	sql := `SELECT DISTINCT table_name
	FROM information_schema.table_constraints as tc 
	WHERE tc.constraint_name = $1;`

	rows, err := db.Query(sql, referenceConstraintName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	var name string
	rows.Next()
	rows.Scan(&name)

	return name
}

func readReferenceConstraints(db *sql.DB, tableName string, referenceConstraintName string) map[string]string {
	sql := `SELECT DISTINCT kcu.column_name, ccu.column_name AS foreign_column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
			AND tc.table_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_name = $1
			AND tc.constraint_name = $2;`

	rows, err := db.Query(sql, tableName, referenceConstraintName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var columnName string
		var foreignColumnName string
		err := rows.Scan(&columnName, &foreignColumnName)
		if err != nil {
			log.Panic().Err(err).Msg("error getting column data")
		}
		result[columnName] = foreignColumnName
	}

	return result
}

func findIndex(indexes map[string]UniqueIndex, columnName string) string {
	for name, index := range indexes {
		for _, column := range index.Columns {
			if strings.Compare(column, columnName) == 0 {
				return name
			}
		}
	}
	return ""
}

func findIndexMostColumns(indexes map[string]UniqueIndex) string {
	mostCols := 0
	result := ""
	for name, index := range indexes {
		numCols := len(index.Columns)
		if numCols > mostCols {
			result = name
			mostCols = numCols
		}
	}
	return result
}

func readPKSequence(db *sql.DB, tableName string) string {
	sql := `WITH sequences AS (
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
				ON replace(regexp_replace(constraint_name, '(_id)?_pk(ey)?', ''), '_', '') = replace(regexp_replace(sequence_name, '(_id)?_seq', ''), '_', '')`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Panic().Err(err).Msg("error executing query")
	}
	defer rows.Close()

	var name string
	rows.Next()
	rows.Scan(&name)

	return name
}

// ReadTablesSchema inspects the DB and returns a list of tables
func ReadAllTablesSchema(db *sql.DB) map[string]Table {
	return ReadTablesSchema(db, readTableNames(db))
}

func ReadTablesSchema(db *sql.DB, tableNames []string) map[string]Table {

	result := make(map[string]Table, 0)
	for _, tableName := range tableNames {
		table, err := processTable(db, strings.ToLower(tableName), true)
		if err {
			continue
		}
		result[table.Name] = table
	}

	//Load all reference tables not loaded yet
	for _, table := range result {
		result = processReferenceTables(db, table, result)
	}

	return result
}

func processReferenceTables(db *sql.DB, table Table, currentTables map[string]Table) map[string]Table {
	for _, reference := range table.References {
		_, ok := currentTables[reference.TableName]
		if ok {
			continue
		}
		tableProcessed, _ := processTable(db, reference.TableName, false)
		currentTables[reference.TableName] = tableProcessed
		currentTables = processReferenceTables(db, tableProcessed, currentTables)
	}

	return currentTables
}

func processTable(db *sql.DB, tableName string, exportable bool) (Table, bool) {
	columns := readColumnNames(db, tableName)
	if len(columns) == 0 {
		log.Info().Msgf("Ignoring nonexisting table %s", tableName)
		return Table{}, true
	}

	columnIndexes := make(map[string]int)
	for i, columnName := range columns {
		columnIndexes[columnName] = i
	}

	pkColumns := readPKColumnNames(db, tableName)
	pkColumnMap := make(map[string]bool)
	for _, column := range pkColumns {
		pkColumnMap[column] = true
	}

	pkSequence := readPKSequence(db, tableName)

	indexNames := readUniqueIndexNames(db, tableName)
	indexes := make(map[string]UniqueIndex)
	for _, indexName := range indexNames {
		indexColumns := readIndexColumns(db, indexName)
		indexes[indexName] = UniqueIndex{Name: indexName, Columns: indexColumns}
	}

	mainUniqueIndexName := ""
	if len(indexNames) == 1 {
		mainUniqueIndexName = indexNames[0]
	} else if len(indexNames) > 1 {
		mainUniqueIndexName = findIndex(indexes, "label")
		if len(mainUniqueIndexName) == 0 {
			mainUniqueIndexName = findIndex(indexes, "name")
			if len(mainUniqueIndexName) == 0 {
				mainUniqueIndexName = findIndex(indexes, "token")
				if len(mainUniqueIndexName) == 0 {
				        mainUniqueIndexName = findIndexMostColumns(indexes)
				}
			}
		}
	}

	constraintNames := readReferenceConstraintNames(db, tableName)
	references := make([]Reference, 0)
	for _, constraintName := range constraintNames {
		columnMap := readReferenceConstraints(db, tableName, constraintName)
		referencedTable := readReferencedTable(db, constraintName)
		references = append(references, Reference{TableName: referencedTable, ColumnMapping: columnMap})
	}

	referencedByConstraintNames := readReferencedByConstraintNames(db, tableName)
	referencedBy := make([]Reference, 0)
	for _, constraintName := range referencedByConstraintNames {
		referencedTable := readReferencedByTable(db, constraintName)
		columnMap := readReferenceConstraints(db, referencedTable, constraintName)
		referencedBy = append(referencedBy, Reference{TableName: referencedTable, ColumnMapping: columnMap})
	}

	table := Table{
		Name:                tableName,
		Export:              exportable,
		Columns:             columns,
		ColumnIndexes:       columnIndexes,
		PKColumns:           pkColumnMap,
		PKSequence:          pkSequence,
		UniqueIndexes:       indexes,
		MainUniqueIndexName: mainUniqueIndexName,
		References:          references,
		ReferencedBy:        referencedBy}
	table = applyTableFilters(table)
	return table, false
}
