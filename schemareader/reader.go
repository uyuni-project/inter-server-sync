package schemareader

import (
	"database/sql"
	"log"
)


func tablesLoadStatic() []string {
	return [] string{
		"rhnchannel",
		"rhnchannelarch",
		"rhnchannelerrata",
		"rhnchannelfamily",
		"rhnchannelfamilymembers",
		//"rhnchannelpackage",
		//"rhnchannelpackagearchcompat",
		//"rhnchanneltrust",
		//"rhncontentsource",
		//"rhncontentsourcessl",
		//"rhncpuarch",
		//"rhncve",
		//"rhndistchannelmap",
		//"rhnerrata",
		//"rhnerratabuglist",
		//"rhnerratacve",
		//"rhnerratafile",
		//"rhnerratafilechannel",
		//"rhnerratafilepackage",
		//"rhnerratafilepackagesource",
		//"rhnerratakeyword",
		//"rhnerratapackage",
		//"rhnkickstartabletree",
		//"rhnksinstalltype",
		//"rhnkstreefile",
		//"rhnkstreetype",
		//"rhnpackage",
		//"rhnpackagearch",
		//"rhnpackagebreaks",
		//"rhnpackagechangelogdata",
		//"rhnpackagechangelogrec",
		//"rhnpackageconflicts",
		//"rhnpackageenhances",
		//"rhnpackagefile",
		//"rhnpackageobsoletes",
		//"rhnpackagepredepends",
		//"rhnpackageprovides",
		//"rhnpackagerecommends",
		//"rhnpackagerequires",
		//"rhnpackagesource",
		//"rhnpackagesuggests",
		//"rhnpackagesupplements",
		//"rhnproductname",
		//"rhnreleasechannelmap",
		//"rhnserverarch",
		//"rhnserverchannelarchcompat",
		//"rhnserverpackagearchcompat",
		//"rhnserverservergrouparchcompat",
		//"suseeula",
		//"susepackageeula",
		//"susepackageproductfile",
		//"suseproductfile",
	}
}

func tablesLoadAll(db *sql.DB) []string {
	sql := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_type = 'BASE TABLE';`

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

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

func readTableNames(db *sql.DB, source string) []string {
	switch source {
	case "static":
		return tablesLoadStatic()
	default:
		return tablesLoadAll(db)
	}
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
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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

func readReferenceConstraintNames(db *sql.DB, tableName string) []string {
	sql := `SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`

	rows, err := db.Query(sql, tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

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

func readReferencedTable(db *sql.DB, tableName string, referenceConstraintName string) string {
	sql := `SELECT DISTINCT ccu.table_name
	FROM information_schema.constraint_column_usage AS ccu
	WHERE ccu.constraint_name = $1;`

	rows, err := db.Query(sql, referenceConstraintName)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var columnName string
		var foreignColumnName string
		err := rows.Scan(&columnName, &foreignColumnName)
		if err != nil {
			log.Fatal(err)
		}
		result[columnName] = foreignColumnName
	}

	return result
}

// ReadTables inspects the DB and returns a list of tables
func ReadTables(db *sql.DB, tablesSource string) []Table {
	tableNames := readTableNames(db, tablesSource)

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

		constraintNames := readReferenceConstraintNames(db, tableName)
		references := make([]Reference, 0)
		for _, constraintName := range constraintNames {
			referencedTable := readReferencedTable(db, tableName, constraintName)
			columnMap := readReferenceConstraints(db, tableName, constraintName)
			references = append(references, Reference{TableName: referencedTable, ColumnMapping: columnMap})
		}

		result = append(result, Table{Name: tableName, Columns: columns, PKColumns: pkColumnMap, UniqueIndexes: indexes, References: references})
	}
	return result
}
