package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var cache = make(map[string]string)

func PrintTableDataOrdered(db *sql.DB, writter *bufio.Writer, schemaMetadata map[string]schemareader.Table, startingTable schemareader.Table, data DataDumper) int {
	result := printTableData(db, writter, schemaMetadata, data, startingTable, make(map[string]bool), make([]string, 0))
	return result
}

func printTableData(db *sql.DB, writter *bufio.Writer, schemaMetadata map[string]schemareader.Table, data DataDumper, table schemareader.Table, processedTables map[string]bool, path []string) int {

	result := 0
	_, tableProcessed := processedTables[table.Name]
	processedTables[table.Name] = true
	path = append(path, table.Name)

	currentTable := schemaMetadata[table.Name]
	tableData, dataOK := data.TableData[table.Name]
	if !dataOK || tableProcessed || !currentTable.Export {
		return result
	}

	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		result = result + printTableData(db, writter, schemaMetadata, data, tableReference, processedTables, path)
	}

	if utils.Contains(TablesToClean, table.Name) {
		rowsValues := make([][]rowDataStructure, 0)
		for _, key := range tableData.Keys {
			rows := getRows(db, table, key)
			for _, row := range rows {
				result++
				values := substituteKeys(db, table, row, schemaMetadata)
				rowsValues = append(rowsValues, values)
			}
		}
		rowToInsert := generateInsertWithClean(rowsValues, table, path, schemaMetadata)
		writter.WriteString(rowToInsert + "\n")
	} else {
		for _, key := range tableData.Keys {
			rows := getRows(db, table, key)
			for _, row := range rows {
				result++
				rowToInsert := prepareRowInsert(db, table, row, schemaMetadata)
				writter.WriteString(rowToInsert + "\n")
			}
		}
	}

	for _, reference := range table.ReferencedBy {

		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		if !shouldFollowReferenceToLink(path, table, tableReference) {
			continue
		}
		result = result + printTableData(db, writter, schemaMetadata, data, tableReference, processedTables, path)
	}
	return result
}

func getRows(db *sql.DB, table schemareader.Table, key TableKey) [][]rowDataStructure {
	whereParameters := make([]string, 0)
	scanParameters := make([]interface{}, 0)
	for column, value := range key.key {
		whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", column, len(whereParameters)+1))
		scanParameters = append(scanParameters, value)
	}
	formattedColumns := strings.Join(table.Columns, ", ")
	formatedWhereParameters := strings.Join(whereParameters, " and ")

	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, table.Name, formatedWhereParameters)
	rows := executeQueryWithResults(db, sql, scanParameters...)
	return rows
}

func substituteKeys(db *sql.DB, table schemareader.Table, row []rowDataStructure, tableMap map[string]schemareader.Table) []rowDataStructure {
	values := substitutePrimaryKey(table, row)
	values = substituteForeignKey(db, table, tableMap, values)
	return values
}

func prepareRowInsert(db *sql.DB, table schemareader.Table, row []rowDataStructure, tableMap map[string]schemareader.Table) string {
	values := substituteKeys(db, table, row, tableMap)
	return generateInsertStatement(values, table)
}

func substitutePrimaryKey(table schemareader.Table, row []rowDataStructure) []rowDataStructure {
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
	return rowResult
}

func substituteForeignKey(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, row []rowDataStructure) []rowDataStructure {
	for _, reference := range table.References {
		row = substituteForeignKeyReference(db, table, tables, reference, row)
	}
	return row
}

func substituteForeignKeyReference(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, reference schemareader.Reference, row []rowDataStructure) []rowDataStructure {
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
		scanParameters = append(scanParameters, row[table.ColumnIndexes[localColumn]].value)
	}

	formattedColumns := strings.Join(foreignTable.Columns, ", ")
	formatedWhereParameters := strings.Join(whereParameters, " and ")

	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
	key := fmt.Sprintf("%s,%s,%s", reference.TableName, formatedWhereParameters, scanParameters)

	cachedValue, found := cache[key]

	if found {
		//Assuming there will be one entry in reference.ColumnMapping
		row[table.ColumnIndexes[localColumns[0]]].value = cachedValue
		row[table.ColumnIndexes[localColumns[0]]].columnType = "SQL"

	} else {
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
								//copiedrow := make([]rowDataStructure, len(rows[0]))
								//copy(copiedrow, rows[0])
								rowResultTemp := substituteForeignKeyReference(db, foreignTable, tables, foreignReference, rows[0])
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
				row[table.ColumnIndexes[localColumn]].value = updatSql
				row[table.ColumnIndexes[localColumn]].columnType = "SQL"
				cache[key] = updatSql
			}
		}
	}
	return row
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

func formatColumnAssignment(table schemareader.Table) string {
	assignments := make([]string, 0)
	for _, column := range table.Columns {
		if !table.PKColumns[column] {
			assignments = append(assignments, fmt.Sprintf("%s = excluded.%s", column, column))
		}
	}
	return strings.Join(assignments, ",")
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
	case "rhnpackagecapability":
		var version interface{} = nil
		for _, field := range row {
			if strings.Compare(field.columnName, "version") == 0 {
				version = field.value
			}
		}
		if version == nil {
			return "(name) WHERE version IS NULL DO NOTHING"
		} else {
			return "(name, version) WHERE version IS NOT NULL DO NOTHING"
		}
	}
	columnAssignment := formatColumnAssignment(table)
	return fmt.Sprintf("%s DO UPDATE SET %s", constraint, columnAssignment)
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
func buildQueryToGetExistingRecords(path []string, table schemareader.Table, schemaMetadata map[string]schemareader.Table) string {
	mainUniqueColumns := strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ",")
	joinsClause := getJoinsClause(path, table, schemaMetadata)
	whereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = %s`, "replace-me)")
	return fmt.Sprintf(`SELECT %s.%s FROM %s %s %s`, table.Name, mainUniqueColumns, table.Name, joinsClause, whereClause)
}

func getJoinsClause(path []string, table schemareader.Table, schemaMetadata map[string]schemareader.Table) string {
	var result strings.Builder
	utils.ReverseArray(path)
	log.Printf("%s", path)
	for i := 0; i < len(path)-1; i++ {
		firstTable := path[i]
		secondTable := path[i+1]
		reverseRelationLookup := false
		relationFound := findRelationInfo(schemaMetadata[firstTable].ReferencedBy, secondTable)
		if relationFound == nil {
			relationFound = findRelationInfo(schemaMetadata[firstTable].References, secondTable)
			reverseRelationLookup = true
		}
		for key, value := range relationFound {
			if reverseRelationLookup {
				result.WriteString(fmt.Sprintf(` INNER JOIN %s on %s.%s = %s.%s`, secondTable, secondTable, value, firstTable, key))
			} else {
				result.WriteString(fmt.Sprintf(` INNER JOIN %s on %s.%s = %s.%s`, secondTable, secondTable, key, firstTable, value))
			}

		}
	}

	return result.String()
}

func findRelationInfo(References []schemareader.Reference, tableToFind string) map[string]string {
	for _, reference := range References {

		if reference.TableName == tableToFind {
			return reference.ColumnMapping
		}
	}
	return nil
}

func generateInsertStatement(values []rowDataStructure, table schemareader.Table) string {
	tableName := table.Name
	columnNames := strings.Join(table.Columns, ", ")
	valueFiltered := filterRowData(values, table)

	if strings.Compare(table.MainUniqueIndexName, schemareader.VirtualIndexName) == 0 {
		whereClauseList := make([]string, 0)
		for _, indexColumn := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
			for _, value := range values {
				if strings.Compare(indexColumn, value.columnName) == 0 {
					if value.value == nil {
						whereClauseList = append(whereClauseList, fmt.Sprintf(" %s IS NULL", value.columnName))
					} else {
						whereClauseList = append(whereClauseList, fmt.Sprintf(" %s = %s",
							value.columnName, formatField(value)))
					}
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
func generateInsertWithClean(values [][]rowDataStructure, table schemareader.Table, path []string, schemaMetadata map[string]schemareader.Table) string {

	var valueFiltered []string
	for _, rowValue := range values {
		valueFiltered = append(valueFiltered, "("+formatValue(rowValue)+")")

	}
	allValues := strings.Join(valueFiltered, ", ")

	tableName := table.Name
	columnNames := strings.Join(table.Columns, ", ")
	onConflictFormated := formatOnConflict(values[0], table)

	mainUniqueColumns := strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ",")

	insertPart := fmt.Sprintf(`WITH new_records_%s AS (INSERT INTO %s (%s) VALUES %s  ON CONFLICT %s RETURNING %s)`,
		tableName, tableName, columnNames, allValues, onConflictFormated, mainUniqueColumns)

	existingRecords := buildQueryToGetExistingRecords(path, table, schemaMetadata)

	deletePart := fmt.Sprintf("\nDELETE FROM %s WHERE (%s) IN (SELECT * FROM existing_records_%s EXCEPT ALL SELECT * FROM new_records_%s);", tableName, mainUniqueColumns, tableName, tableName)
	finalQuery := fmt.Sprintf(`%s, existing_records_%s as (%s) %s`, insertPart, tableName, existingRecords, deletePart)
	log.Printf(finalQuery)
	return finalQuery

}
