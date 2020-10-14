package dumper

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/moio/mgr-dump/schemareader"
)

type rowDataStructure struct {
	columnName string
	columnType string
	value      interface{} // we probably should not save this here, and maybe we should just save a string...
}

// Dump creates a SQL representation of data in the schema
func Dump(db *sql.DB, tables []schemareader.Table) string {
	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}

	table := tables[0]

	tableName := table.Name
	columnNames := strings.Join(table.Columns, ", ")
	values := dumpValues(db, table, tables)
	values = substituteForeignKeys(db, table, tableMap, values)
	formattedValues := formatValues(values)
	constraint := table.MainUniqueIndexName
	columnAssignment := formatColumnAssignment(table)

	return fmt.Sprintf(`INSERT INTO %s (
		%s
	)
	VALUES
		%s
	ON CONFLICT ON CONSTRAINT %s DO UPDATE
		SET %s;`, tableName, columnNames, formattedValues, constraint, columnAssignment)
}

func substituteForeignKeys(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, rows [][]rowDataStructure) [][]rowDataStructure {
	result := make([][]rowDataStructure, 0)

	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	for _, row := range rows {
		rowResult := make([]rowDataStructure, 0)
		for _, column := range row {
			rowResult = append(rowResult, column)
		}

		for _, reference := range table.References {
			foreignTable := tables[reference.TableName]

			foreignMainUniqueColumns := foreignTable.UniqueIndexes[foreignTable.MainUniqueIndexName].Columns
			localColumns := make([]string, 0)
			foreignColumns := make([]string, 0)

			whereParameters := make([]string, 0)
			scanParameters := make([]interface{}, 0)
			for localColumn, foreignColumn := range reference.ColumnMapping {
				localColumns = append(localColumns, localColumn)
				foreignColumns = append(foreignColumns, foreignColumn)

				whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", foreignColumn, len(whereParameters)+1))
				scanParameters = append(scanParameters, row[columnIndexes[localColumn]].value)
			}

			formattedColumns := strings.Join(foreignMainUniqueColumns, ", ")
			formatedWhereParameters := strings.Join(whereParameters, " and ")

			sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)

			rows := executeQueryWithResults(db, sql, scanParameters...)

			// for each localColumns
			//foreignColumns.names -> rows.results
			whereParameters = make([]string, 0)
			for _, foreignColumn := range foreignMainUniqueColumns {
				// produce the where clause
				for _, c := range rows[0] {
					if strings.Compare(c.columnName, foreignColumn) == 0 {
						whereParameters = append(whereParameters, fmt.Sprintf("%s = %s",
							foreignColumn, formatValue(c)))
						break
					}
				}

			}
			for localColumn, foreignColumn := range reference.ColumnMapping {
				updatSql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s`, foreignColumn, reference.TableName, strings.Join(whereParameters, " and "))

				rowResult[columnIndexes[localColumn]].value = updatSql
				rowResult[columnIndexes[localColumn]].columnType = "SQL"
			}

		}

		result = append(result, rowResult)
	}

	return result
}

func formatColumnAssignment(table schemareader.Table) string {
	assignments := make([]string, 0)
	for _, column := range table.Columns {
		if !table.PKColumns[column] {
			assignments = append(assignments, fmt.Sprintf("%s = excluded.%s", column, column))
		}
	}
	return strings.Join(assignments, ",\n")
}

func formatValues(values [][]rowDataStructure) string {
	result := make([]string, 0)
	for _, row := range values {
		listData := make([]string, 0)
		for _, col := range row {
			val := formatValue(col)

			listData = append(listData, val)
		}
		result = append(result, "("+strings.Join(listData, ",")+")")
	}
	return strings.Join(result, ",")
}

func formatValue(col rowDataStructure) string {
	val := ""
	switch col.columnType {
	case "NUMERIC":
		val = fmt.Sprintf(`%s`, col.value)
	case "TIMESTAMPTZ":
		val = pq.QuoteLiteral(string(pq.FormatTimestamp(col.value.(time.Time))))
	case "SQL":
		val = fmt.Sprintf(`(%s)`, col.value)
	default:
		val = pq.QuoteLiteral(fmt.Sprintf("%s", col.value))
	}
	return val
}

func dumpValues(db *sql.DB, table schemareader.Table, tables []schemareader.Table) [][]rowDataStructure {

	columnNames := strings.Join(table.Columns, ", ")

	sql := fmt.Sprintf(`SELECT %s FROM %s ;`, columnNames, table.Name)
	return executeQueryWithResults(db, sql)
}

func executeQueryWithResults(db *sql.DB, sql string, scanParameters ...interface{}) [][]rowDataStructure {

	rows, err := db.Query(sql, scanParameters...)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// get column type info
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err)
	}

	// used for allocation & dereferencing
	rowValues := make([]reflect.Value, len(columnTypes))
	for i := 0; i < len(columnTypes); i++ {
		// allocate reflect.Value representing a **T value
		rowValues[i] = reflect.New(reflect.PtrTo(columnTypes[i].ScanType()))
	}

	computedValues := make([][]rowDataStructure, 0)
	for rows.Next() {
		// initially will hold pointers for Scan, after scanning the
		// pointers will be dereferenced so that the slice holds actual values
		rowResult := make([]interface{}, len(columnTypes))
		for i := 0; i < len(columnTypes); i++ {
			// get the **T value from the reflect.Value
			rowResult[i] = rowValues[i].Interface()
		}

		// scan each column value into the corresponding **T value
		if err := rows.Scan(rowResult...); err != nil {
			log.Fatal(err)
		}

		// dereference pointers
		rowComputedValues := make([]rowDataStructure, 0)
		for i := 0; i < len(rowValues); i++ {
			// first pointer deref to get reflect.Value representing a *T value,
			// if rv.IsNil it means column value was NULL
			if rv := rowValues[i].Elem(); rv.IsNil() {
				rowResult[i] = nil
			} else {
				// second deref to get reflect.Value representing the T value
				// and call Interface to get T value from the reflect.Value
				rowResult[i] = rv.Elem().Interface()
				rowComputedValues = append(rowComputedValues, rowDataStructure{columnType: columnTypes[i].DatabaseTypeName(),
					value: rowResult[i], columnName: columnTypes[i].Name()})
			}
		}

		computedValues = append(computedValues, rowComputedValues)
	}

	return computedValues
}
