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
	columnName   string
	columnType   string
	initialValue interface{}
	value        interface{}
}

// Dump creates a SQL representation of data in the schema
func Dump(db *sql.DB, tables []schemareader.Table) []string {
	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	result := make([]string, 0)

	for i, table := range tables {
		if i >= 26 {
			break
		}
		values := dumpValues(db, table, tables)
		values = substitutePrimaryKeys(db, table, tableMap, values)
		values = substituteForeignKeys(db, table, tableMap, values)

		for _, value := range values {
			insertStatement := generateInsertStatement(value, table)
			result = append(result, insertStatement)

		}
	}

	return result
}

func generateInsertStatement(values []rowDataStructure, table schemareader.Table) string {
	tableName := table.Name
	columnNames := strings.Join(table.Columns, ", ")
	valueFiltered := filterRowData(values, table)
	if strings.Compare(tableName, "rhnpackage") == 0 {

		//select nextval('rhn_checksum_id_seq'), 1, 1,
		//	'2020-10-20 09:47:40.587718 +00:00', '2020-10-20 09:47:40.587718', '2020-10-20 09:47:40.587718'
		//	where not exists (select 1
		//		from my_table where org_id = 1 and modified = '2020-10-20 09:47:40.587718' );
		whereClauseList := make([]string, 0)
		for _, value := range values {
			switch value.columnName {
			case "name_id", "evr_id", "package_arch_id", "checksum_id":
				whereClauseList = append(whereClauseList, fmt.Sprintf(" %s = %s",
					value.columnName, formatField(value)))
				//'org_id', 'name_id', 'evr_id', 'package_arch_id','checksum_id'
			case "org_id":
				if value.value == nil {
					whereClauseList = append(whereClauseList, fmt.Sprintf(" %s IS NULL", value.columnName))
				} else {
					whereClauseList = append(whereClauseList, fmt.Sprintf(" %s = %s",
						value.columnName, formatField(value)))
				}
			}
		}
		whereClause := strings.Join(whereClauseList, " and ")
		return fmt.Sprintf(`INSERT INTO %s (%s)	select %s  where  not exists (select 1 from %s where %s);`,
			tableName, columnNames, formatValue(valueFiltered), tableName, whereClause)
	} else {
		onConflictFormated := formatOnConflict(values, table)
		return fmt.Sprintf(`INSERT INTO %s (%s)	VALUES (%s)  ON CONFLICT %s ;`,
			tableName, columnNames, formatValue(valueFiltered), onConflictFormated)
	}

}

func filterRowData(value []rowDataStructure, table schemareader.Table) []rowDataStructure {
	if strings.Compare(table.Name, "rhnerrata") == 0 {
		for i, row := range value {
			if strings.Compare(row.columnName, "severity_id") == 0 {
				value[i].value = value[i].initialValue
			}
		}
	}
	return value
}

func formatOnConflict(row []rowDataStructure, table schemareader.Table) string {
	constraint := "(" + strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ", ") + ")"
	switch table.Name {
	case "rhnerrataseverity":
		constraint = "(id)"
	case "rhnerrata":
		// TODO rhnerrata and rhnpackageevr logic is similar, so we extract to one method on future
		var orgId interface{} = nil
		for _, field := range row {
			if strings.Compare(field.columnName, "org_id") == 0 {
				orgId = field.value
			}
		}
		if orgId == nil {
			constraint = "(advisory) WHERE org_id IS NULL"
		} else {
			constraint = "(advisory, org_id) WHERE org_id IS NOT NULL"
		}
	case "rhnpackageevr":
		var epoch interface{} = nil
		for _, field := range row {
			if strings.Compare(field.columnName, "epoch") == 0 {
				epoch = field.value
			}
		}
		if epoch == nil {
			return "(version, release) WHERE epoch IS NULL DO NOTHING"
		} else {
			return "(version, release, epoch) WHERE epoch IS NOT NULL DO NOTHING"
		}
	}
	columnAssignment := formatColumnAssignment(table)
	return fmt.Sprintf("%s DO UPDATE SET %s", constraint, columnAssignment)
}

func substitutePrimaryKeys(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, rows [][]rowDataStructure) [][]rowDataStructure {
	result := make([][]rowDataStructure, 0)
	for _, row := range rows {
		rowResult := make([]rowDataStructure, 0)
		pkSequence := false
		if len(table.PKSequence) > 0 {
			pkSequence = true
		}
		for _, column := range row {
			if pkSequence && strings.Compare(column.columnName, "id") == 0 {
				column.columnType = "SQL"
				column.value = fmt.Sprintf("SELECT nextval('%s')", table.PKSequence)
				rowResult = append(rowResult, column)
			} else {
				rowResult = append(rowResult, column)
			}
		}
		result = append(result, rowResult)
	}
	return result
}

func substituteForeignKeys(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, rows [][]rowDataStructure) [][]rowDataStructure {
	result := make([][]rowDataStructure, 0)

	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}
	for _, row := range rows {

		for _, reference := range table.References {
			row = substituteForeignKeyReference(db, tables, reference, row, columnIndexes)
		}

		result = append(result, row)
	}

	return result
}

func substituteForeignKeyReference(db *sql.DB, tables map[string]schemareader.Table, reference schemareader.Reference, row []rowDataStructure, columnIndexes map[string]int) []rowDataStructure {
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

	formattedColumns := strings.Join(foreignTable.Columns, ", ")
	formatedWhereParameters := strings.Join(whereParameters, " and ")

	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)

	rows := executeQueryWithResults(db, sql, scanParameters...)

	// we will only change for a sub query if we were able to find the target value
	// other wise we keep the pre existing value.
	// this can happen when the column for the reference is null. Example rhnchanel->org_id
	if len(rows) > 0 {
		whereParameters = make([]string, 0)

		for _, foreignColumn := range foreignMainUniqueColumns {
			// produce the where clause
			for _, c := range rows[0] {
				if strings.Compare(c.columnName, foreignColumn) == 0 {
					if c.value == nil {
						whereParameters = append(whereParameters, fmt.Sprintf("%s is null",
							foreignColumn))
					} else {
						foreignReference := foreignTable.GetFirstReferenceFromColumn(foreignColumn)
						if strings.Compare(foreignReference.TableName, "") == 0 {
							whereParameters = append(whereParameters, fmt.Sprintf("%s = %s",
								foreignColumn, formatField(c)))
						} else {
							columnIndexesForeign := make(map[string]int)
							for i, columnName := range foreignTable.Columns {
								columnIndexesForeign[columnName] = i
							}
							rowResultTemp := substituteForeignKeyReference(db, tables, foreignReference, rows[0], columnIndexesForeign)
							fieldToUpdate := formatField(c)
							for _, field := range rowResultTemp {
								if strings.Compare(field.columnName, foreignColumn) == 0 {
									fieldToUpdate = formatField(field)
									break
								}
							}
							whereParameters = append(whereParameters, fmt.Sprintf("%s = %s",
								foreignColumn, fieldToUpdate))
						}

					}
					break
				}
			}

		}
		for localColumn, foreignColumn := range reference.ColumnMapping {
			updatSql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s limit 1`, foreignColumn, reference.TableName, strings.Join(whereParameters, " and "))

			row[columnIndexes[localColumn]].value = updatSql
			row[columnIndexes[localColumn]].columnType = "SQL"
		}
	}
	return row
}

func formatColumnAssignment(table schemareader.Table) string {
	assignments := make([]string, 0)
	for _, column := range table.Columns {
		if !table.PKColumns[column] {
			assignments = append(assignments, fmt.Sprintf("%s = excluded.%s", column, column))
		}
	}
	return strings.Join(assignments, ",")
}

func formatValue(value []rowDataStructure) string {
	result := make([]string, 0)
	for _, col := range value {
		result = append(result, formatField(col))
	}
	return strings.Join(result, ",")
}

func formatField(col rowDataStructure) string {
	if col.value == nil {
		return "null"
	}
	val := ""
	switch col.columnType {
	case "NUMERIC":
		val = fmt.Sprintf(`%s`, col.value)
	case "TIMESTAMPTZ", "TIMESTAMP":
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
			}
			rowComputedValues = append(rowComputedValues, rowDataStructure{columnType: columnTypes[i].DatabaseTypeName(),
				initialValue: rowResult[i], value: rowResult[i], columnName: columnTypes[i].Name()})
		}

		computedValues = append(computedValues, rowComputedValues)
	}

	return computedValues
}
