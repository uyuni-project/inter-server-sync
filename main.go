package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// cd spacewalk/java; make -f Makefile.docker dockerrun_pg
const connectionString = "user='spacewalk' password='spacewalk' dbname='susemanager' host='localhost' port='5432' sslmode=disable"

// Table represents a DB table to dump
type Table struct {
	name    string
	columns []string
}

func readTableNames(db *sql.DB) []string {
	sql := `SELECT table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE';`

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

func readTables(db *sql.DB) []Table {
	tableNames := readTableNames(db)

	result := make([]Table, 0)
	for _, tableName := range tableNames {
		columns := readColumnNames(db, tableName)

		result = append(result, Table{name: tableName, columns: columns})
	}
	return result
}

func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	tables := readTables(db)

	for _, table := range tables {
		fmt.Println(table.name)

		for _, column := range table.columns {
			fmt.Printf("  - %s\n", column)
		}
	}
}
