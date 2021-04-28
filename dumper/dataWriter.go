package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/uyuni-project/inter-server-sync/sqlUtil"

	"github.com/lib/pq"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var cache = make(map[string]string)

func PrintTableDataOrdered(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
	startingTable schemareader.Table, data DataDumper, options PrintSqlOptions) {

	printTableData(db, writer, schemaMetadata, data, startingTable, make(map[string]bool), make([]string, 0), options)
}

func printTableData(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, data DataDumper,
	table schemareader.Table, processedTables map[string]bool, path []string, options PrintSqlOptions) {

	_, tableProcessed := processedTables[table.Name]
	// if the current table should not be export we are interrupting the crawler process for these table
	// not exporting other tables relations
	if tableProcessed || !table.Export {
		return
	}
	processedTables[table.Name] = true
	path = append(path, table.Name)

	// this should be moved to section process current table, and we should follow links
	tableData, dataOK := data.TableData[table.Name]
	if !dataOK {
		if utils.Contains(options.TablesToClean, table.Name) {
			cleanEmptyTable := generateClearEmptyTable(table, path, schemaMetadata, options)
			writer.WriteString(cleanEmptyTable + "\n")
			return
		} else {
			return
		}
	}

	// follow reference to
	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		printTableData(db, writer, schemaMetadata, data, tableReference, processedTables, path, options)
	}

	// export current table data
	rows := GetRowsFromKeys(db, schemaMetadata, tableData)
	if utils.Contains(options.TablesToClean, table.Name) {
		rowToInsert := generateInsertWithClean(db, rows, table, path, schemaMetadata, options.CleanWhereClause)
		writer.WriteString(rowToInsert + "\n")
	} else {
		for _, rowValue := range rows {
			rowToInsert := generateRowInsertStatement(db, rowValue, table, schemaMetadata, options.OnlyIfParentExistsTables)
			writer.WriteString(rowToInsert + "\n")
		}
	}

	// follow reference by
	for _, reference := range table.ReferencedBy {

		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		if !shouldFollowReferenceToLink(path, table, tableReference) {
			continue
		}
		printTableData(db, writer, schemaMetadata, data, tableReference, processedTables, path, options)
	}
}

// GetRowsFromKeys check if we should move this to a method in the type tableData
func GetRowsFromKeys(db *sql.DB, schemaMetadata map[string]schemareader.Table, tableData TableDump) [][]sqlUtil.RowDataStructure {
	rowsResult := make([][]sqlUtil.RowDataStructure, 0)
	if len(tableData.Keys) == 0 {
		return rowsResult
	}
	table := schemaMetadata[tableData.TableName]
	formattedColumns := strings.Join(table.Columns, ", ")

	columnsFilter := make([]string, 0)
	for column, _ := range tableData.Keys[0].Key {
		columnsFilter = append(columnsFilter, column)
	}
	values := make([]string, 0)
	for _, key := range tableData.Keys {
		row := make([]string, 0)
		for _, c := range columnsFilter {
			row = append(row, key.Key[c])
		}

		values = append(values, "("+strings.Join(row, ",")+")")
		// FIXME the query value should be a parameter
		if len(values) >= 1000 {
			// FIXME query should be defined one time, instead of being replicate some lines bellow
			sql := fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) in (%s);`,
				formattedColumns, table.Name, strings.Join(columnsFilter, ", "), strings.Join(values, ","))
			rowsResult = append(rowsResult, sqlUtil.ExecuteQueryWithResults(db, sql)...)
			values = make([]string, 0)
		}
	}
	if len(values) > 0 {
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) in (%s);`,
			formattedColumns, table.Name, strings.Join(columnsFilter, ", "), strings.Join(values, ","))
		rowsResult = append(rowsResult, sqlUtil.ExecuteQueryWithResults(db, sql)...)

	}
	return rowsResult
}

func filterRowData(value []sqlUtil.RowDataStructure, table schemareader.Table) []sqlUtil.RowDataStructure {
	if strings.Compare(table.Name, "rhnerrata") == 0 {
		for i, row := range value {
			if strings.Compare(row.ColumnName, "severity_id") == 0 {
				value[i].Value = value[i].GetInitialValue()
			}
		}
	}
	if table.UnexportColumns != nil {
		returnValues := make([]sqlUtil.RowDataStructure, 0)
		for _, row := range value {
			_, ok := table.UnexportColumns[row.ColumnName]
			if !ok {
				returnValues = append(returnValues, row)
			}
		}
		return returnValues
	}
	return value
}

func substituteKeys(db *sql.DB, table schemareader.Table, row []sqlUtil.RowDataStructure, tableMap map[string]schemareader.Table) []sqlUtil.RowDataStructure {
	values := substitutePrimaryKey(table, row)
	values = substituteForeignKey(db, table, tableMap, values)
	return values
}

func substitutePrimaryKey(table schemareader.Table, row []sqlUtil.RowDataStructure) []sqlUtil.RowDataStructure {
	rowResult := make([]sqlUtil.RowDataStructure, 0)
	pkSequence := false
	if len(table.PKSequence) > 0 {
		pkSequence = true
	}
	for _, column := range row {
		if pkSequence && strings.Compare(column.ColumnName, "id") == 0 {
			column.ColumnType = "SQL"
			column.Value = fmt.Sprintf("SELECT nextval('%s')", table.PKSequence)
			rowResult = append(rowResult, column)
		} else {
			rowResult = append(rowResult, column)
		}
	}
	return rowResult
}

func substituteForeignKey(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, row []sqlUtil.RowDataStructure) []sqlUtil.RowDataStructure {
	for _, reference := range table.References {
		row = substituteForeignKeyReference(db, table, tables, reference, row)
	}
	return row
}

func substituteForeignKeyReference(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, reference schemareader.Reference, row []sqlUtil.RowDataStructure) []sqlUtil.RowDataStructure {
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
		scanParameters = append(scanParameters, row[table.ColumnIndexes[localColumn]].Value)
	}

	formattedColumns := strings.Join(foreignTable.Columns, ", ")
	formattedWhereParameters := strings.Join(whereParameters, " and ")

	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formattedWhereParameters)
	key := fmt.Sprintf("%s,%s,%s", reference.TableName, formattedWhereParameters, scanParameters)

	cachedValue, found := cache[key]

	if found {
		//Assuming there will be one entry in reference.ColumnMapping
		row[table.ColumnIndexes[localColumns[0]]].Value = cachedValue
		row[table.ColumnIndexes[localColumns[0]]].ColumnType = "SQL"

	} else {
		rows := sqlUtil.ExecuteQueryWithResults(db, sql, scanParameters...)
		// we will only change for a sub query if we were able to find the target Value
		// other wise we keep the pre existing Value.
		// this can happen when the column for the reference is null. Example rhnchanel->org_id
		if len(rows) > 0 {
			whereParameters = make([]string, 0)

			for _, foreignColumn := range foreignMainUniqueColumns {
				// produce the where clause
				for _, c := range rows[0] {
					if strings.Compare(c.ColumnName, foreignColumn) == 0 {
						if c.Value == nil {
							whereParameters = append(whereParameters, fmt.Sprintf("%s is null",
								foreignColumn))
						} else {
							foreignReference := foreignTable.GetFirstReferenceFromColumn(foreignColumn)
							if strings.Compare(foreignReference.TableName, "") == 0 {
								whereParameters = append(whereParameters, fmt.Sprintf("%s = %s",
									foreignColumn, formatField(c)))
							} else {
								//copiedrow := make([]sqlUtil.RowDataStructure, len(rows[0]))
								//copy(copiedrow, rows[0])
								rowResultTemp := substituteForeignKeyReference(db, foreignTable, tables, foreignReference, rows[0])
								fieldToUpdate := formatField(c)
								for _, field := range rowResultTemp {
									if strings.Compare(field.ColumnName, foreignColumn) == 0 {
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
				updateSql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s limit 1`, foreignColumn, reference.TableName, strings.Join(whereParameters, " and "))
				row[table.ColumnIndexes[localColumn]].Value = updateSql
				row[table.ColumnIndexes[localColumn]].ColumnType = "SQL"
				cache[key] = updateSql
			}
		}
	}
	return row
}

func formatRowValue(value []sqlUtil.RowDataStructure) string {
	result := make([]string, 0)
	for _, col := range value {
		result = append(result, formatField(col))
	}
	return strings.Join(result, ",")
}

func formatField(col sqlUtil.RowDataStructure) string {
	if col.Value == nil {
		return "null"
	}
	val := ""
	switch col.ColumnType {
	case "NUMERIC":
		val = fmt.Sprintf(`%s`, col.Value)
	case "TIMESTAMPTZ", "TIMESTAMP":
		val = pq.QuoteLiteral(string(pq.FormatTimestamp(col.Value.(time.Time))))
	case "SQL":
		val = fmt.Sprintf(`(%s)`, col.Value)
	default:
		val = pq.QuoteLiteral(fmt.Sprintf("%s", col.Value))
	}
	return val
}

func formatColumnAssignment(table schemareader.Table) string {
	assignments := make([]string, 0)
	for _, column := range table.Columns {
		if !table.PKColumns[column] && !table.UnexportColumns[column] {
			assignments = append(assignments, fmt.Sprintf("%s = excluded.%s", column, column))
		}
	}
	return strings.Join(assignments, ",")
}

func formatOnConflict(row []sqlUtil.RowDataStructure, table schemareader.Table) string {
	constraint := "(" + strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ", ") + ")"
	switch table.Name {
	case "rhnerrataseverity":
		constraint = "(id)"
	case "rhnerrata":
		// TODO rhnerrata and rhnpackageevr logic is similar, so we extract to one method on future
		var orgId interface{} = nil
		for _, field := range row {
			if strings.Compare(field.ColumnName, "org_id") == 0 {
				orgId = field.Value
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
			if strings.Compare(field.ColumnName, "epoch") == 0 {
				epoch = field.Value
			}
		}
		if epoch == nil {
			return "(version, release, ((evr).type)) WHERE epoch IS NULL DO NOTHING"
		} else {
			return "(version, release, epoch, ((evr).type)) WHERE epoch IS NOT NULL DO NOTHING"
		}
	case "rhnpackagecapability":
		var version interface{} = nil
		for _, field := range row {
			if strings.Compare(field.ColumnName, "version") == 0 {
				version = field.Value
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

func buildQueryToGetExistingRecords(path []string, table schemareader.Table, schemaMetadata map[string]schemareader.Table, cleanWhereClause string) string {
	mainUniqueColumns := ""
	for _, column := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
		if len(mainUniqueColumns) > 0 {
			mainUniqueColumns = mainUniqueColumns + ", "
		}
		mainUniqueColumns = mainUniqueColumns + table.Name + "." + column
	}

	joinsClause := getJoinsClause(path, schemaMetadata)
	return fmt.Sprintf(`SELECT %s FROM %s %s %s`, mainUniqueColumns, table.Name, joinsClause, cleanWhereClause)
}

func getJoinsClause(path []string, schemaMetadata map[string]schemareader.Table) string {
	var result strings.Builder
	reversePath := make([]string, len(path))
	copy(reversePath, path)
	utils.ReverseArray(reversePath)
	log.Printf("%s", reversePath)
	for i := 0; i < len(reversePath)-1; i++ {
		firstTable := reversePath[i]
		secondTable := reversePath[i+1]
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

func prepareColumnNames(table schemareader.Table) string {
	returnColumn := ""
	for _, column := range table.Columns {
		_, ignore := table.UnexportColumns[column]
		if !ignore {
			if len(returnColumn) == 0 {
				returnColumn = returnColumn + column
			} else {
				returnColumn = returnColumn + ", " + column
			}
		}
	}
	return returnColumn
}

func generateRowInsertStatement(db *sql.DB, values []sqlUtil.RowDataStructure, table schemareader.Table,
	schemaMetadata map[string]schemareader.Table, onlyIfParentExistsTables []string) string {

	tableName := table.Name
	columnNames := prepareColumnNames(table)
	rowKeysProcessed := substituteKeys(db, table, values, schemaMetadata)
	valueFiltered := filterRowData(rowKeysProcessed, table)

	if strings.Compare(table.MainUniqueIndexName, schemareader.VirtualIndexName) == 0 || utils.Contains(onlyIfParentExistsTables, table.Name) {
		whereClauseList := make([]string, 0)
		parentsRecordsCheckList := make([]string, 0)
		for _, indexColumn := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
			for _, value := range valueFiltered {
				if strings.Compare(indexColumn, value.ColumnName) == 0 {
					if value.Value == nil {
						whereClauseList = append(whereClauseList, fmt.Sprintf(" %s IS NULL", value.ColumnName))
					} else {
						whereClauseList = append(whereClauseList, fmt.Sprintf(" %s = %s",
							value.ColumnName, formatField(value)))
						parentsRecordsCheckList = append(parentsRecordsCheckList, fmt.Sprintf("exists %s",
							formatField(value)))
					}
				}
			}
		}
		whereClause := strings.Join(whereClauseList, " and ")
		parentRecordsExistsClause := strings.Join(parentsRecordsCheckList, " and ")

		if utils.Contains(onlyIfParentExistsTables, table.Name) {
			return fmt.Sprintf(`INSERT INTO %s (%s)	select %s  where  not exists (select 1 from %s where %s) and %s;`,
				tableName, columnNames, formatRowValue(valueFiltered), tableName, whereClause, parentRecordsExistsClause)
		}

		return fmt.Sprintf(`INSERT INTO %s (%s)	select %s  where  not exists (select 1 from %s where %s);`,
			tableName, columnNames, formatRowValue(valueFiltered), tableName, whereClause)

	} else {
		onConflictFormatted := formatOnConflict(valueFiltered, table)
		return fmt.Sprintf(`INSERT INTO %s (%s)	VALUES (%s)  ON CONFLICT %s ;`,
			tableName, columnNames, formatRowValue(valueFiltered), onConflictFormatted)
	}

}

func generateInsertWithClean(db *sql.DB, values [][]sqlUtil.RowDataStructure, table schemareader.Table, path []string,
	schemaMetadata map[string]schemareader.Table, cleanWhereClause string) string {

	var valueFiltered []string
	for _, rowValue := range values {
		rowKeysProcessed := substituteKeys(db, table, rowValue, schemaMetadata)
		filteredRowValue := filterRowData(rowKeysProcessed, table)
		valueFiltered = append(valueFiltered, "("+formatRowValue(filteredRowValue)+")")

	}
	allValues := strings.Join(valueFiltered, ", ")

	tableName := table.Name
	columnNames := prepareColumnNames(table)
	onConflictFormatted := formatOnConflict(values[0], table)

	mainUniqueColumns := strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ",")

	insertPart := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s  ON CONFLICT %s RETURNING %s`,
		tableName, columnNames, allValues, onConflictFormatted, mainUniqueColumns)

	existingRecords := buildQueryToGetExistingRecords(path, table, schemaMetadata, cleanWhereClause)

	deletePart := fmt.Sprintf("\nDELETE FROM %s WHERE (%s) IN (%s EXCEPT ALL SELECT * FROM new_records_%s);",
		tableName, mainUniqueColumns, existingRecords, tableName)

	finalQuery := fmt.Sprintf(`WITH new_records_%s AS (%s) %s;`,
		tableName, insertPart, deletePart)

	return finalQuery
}

func generateClearEmptyTable(table schemareader.Table, path []string, schemaMetadata map[string]schemareader.Table, options PrintSqlOptions) string {
	existingRecords := buildQueryToGetExistingRecords(path, table, schemaMetadata, options.CleanWhereClause)
	mainUniqueColumns := strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ",")
	return fmt.Sprintf("\nDELETE FROM %s WHERE (%s) IN (%s);",
		table.Name, mainUniqueColumns, existingRecords)
}
