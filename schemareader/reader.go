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

// ReadTables inspects the DB and returns a list of tables
func ReadTables(db *sql.DB) []Table {
	tableNames := readTableNames(db)

	result := make([]Table, 0)
	for _, tableName := range tableNames {
		columns := readColumnNames(db, tableName)

		result = append(result, Table{Name: tableName, Columns: columns})
	}
	return result
}
