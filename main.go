package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// cd spacewalk/java; make -f Makefile.docker dockerrun_pg
const connectionString = "user='spacewalk' password='spacewalk' dbname='susemanager' host='localhost' port='5432' sslmode=disable"

func main() {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	sql := `SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name IN (
				SELECT table_name
					FROM information_schema.tables
					WHERE table_type = 'BASE TABLE'
			)
		ORDER BY table_name, ordinal_position;`

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var tableName string
		var cloumnName string
		err := rows.Scan(&tableName, &cloumnName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s, %s\n", tableName, cloumnName)
	}
}
