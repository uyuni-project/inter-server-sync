package schemareader

import (
	"database/sql"
	"log"
)

func readTableNames(db *sql.DB) []string {
	sql := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_type = 'BASE TABLE';`

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}

	result := make([]string, 0)
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}

	result := make([]string, 0)
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}

	result := make([]string, 0)
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}

	result := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		result = append(result, name)
	}

	return result
}

// ReadTables inspects the DB and returns a list of tables
func ReadTables(db *sql.DB) []Table {
	tableNames := readTableNames(db)

	result := make([]Table, 0)
	for _, tableName := range tableNames {
		columns := readColumnNames(db, tableName)

		pkColumns := readPKColumnNames(db, tableName)
		pkColumnMap := make(map[string]bool)
		for _, column := range pkColumns {
			pkColumnMap[column] = true
		}

		indexNames := readUniqueIndexNames(db, tableName)
		indexes := make([]UniqueIndex, 0)
		for _, indexName := range indexNames {
			indexColumns := readIndexColumns(db, indexName)
			indexes = append(indexes, UniqueIndex{Name: indexName, Columns: indexColumns})
		}

		result = append(result, Table{Name: tableName, Columns: columns, PKColumns: pkColumnMap, UniqueIndexes: indexes})
	}
	return result
}
