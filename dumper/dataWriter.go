package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
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

	printCleanTables(db, writer, schemaMetadata, startingTable, make(map[string]bool), make([]string, 0), options)
	printTableData(db, writer, schemaMetadata, data, startingTable, make(map[string]bool), make([]string, 0), options)
	cache = make(map[string]string)
}

/**
clear tables need to be printed in reverse order, otherwise it will not work
*/
func printCleanTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, table schemareader.Table,
	processedTables map[string]bool, path []string, options PrintSqlOptions) {

	_, tableProcessed := processedTables[table.Name]
	// if the current table should not be export we are interrupting the crawler process for these table
	// not exporting other tables relations
	if tableProcessed || !table.Export {
		return
	}
	processedTables[table.Name] = true
	path = append(path, table.Name)

	// follow reference by
	for _, reference := range table.ReferencedBy {

		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		if !shouldFollowReferenceToLink(path, table, tableReference) {
			continue
		}
		printCleanTables(db, writer, schemaMetadata, tableReference, processedTables, path, options)
	}

	if utils.Contains(options.TablesToClean, table.Name) {
		generateClearTable(db, writer, table, path, schemaMetadata, options)
	}

	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if !ok || !tableReference.Export {
			continue
		}
		printCleanTables(db, writer, schemaMetadata, tableReference, processedTables, path, options)
	}
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

	// follow reference to
	for _, reference := range table.References {
		tableReference, ok := schemaMetadata[reference.TableName]
		if ok && tableReference.Export && shouldFollowToLinkPreOrder(path, table, tableReference) {
			printTableData(db, writer, schemaMetadata, data, tableReference, processedTables, path, options)
		}
	}

	exportCurrentTableData(db, writer, schemaMetadata, table, data, options)

	// follow reference by
	for _, reference := range table.ReferencedBy {

		tableReference, ok := schemaMetadata[reference.TableName]
		if ok && tableReference.Export && shouldFollowReferenceToLink(path, table, tableReference) {
			printTableData(db, writer, schemaMetadata, data, tableReference, processedTables, path, options)
		}

	}
	if options.PostOrderCallback != nil {
		options.PostOrderCallback(db, writer, schemaMetadata, table, data)
	}
}

func exportCurrentTableData(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
	table schemareader.Table, data DataDumper, options PrintSqlOptions) {

	tableData, dataOK := data.TableData[table.Name]
	if dataOK {
		exportPoint := 0
		batch := 100
		for len(tableData.Keys) > exportPoint {
			upperLimit := exportPoint + batch
			if upperLimit > len(tableData.Keys) {
				upperLimit = len(tableData.Keys)
			}
			rows := GetRowsFromKeys(db, table, tableData.Keys[exportPoint:upperLimit])
			for _, rowValue := range rows {
				rowToInsert := generateRowInsertStatement(db, rowValue, table, schemaMetadata, options.OnlyIfParentExistsTables)
				writer.WriteString(rowToInsert + "\n")
			}
			exportPoint = upperLimit
		}
	}
}

// GetRowsFromKeys check if we should move this to a method in the type tableData
func GetRowsFromKeys(db *sql.DB, table schemareader.Table, keys []TableKey) [][]sqlUtil.RowDataStructure {
	if len(keys) == 0 {
		return make([][]sqlUtil.RowDataStructure, 0)
	}
	formattedColumns := strings.Join(table.Columns, ", ")

	columnsFilter := make([]string, 0)
	for column, _ := range keys[0].Key {
		columnsFilter = append(columnsFilter, column)
	}
	values := make([]string, 0)
	for _, key := range keys {
		row := make([]string, 0)
		for _, c := range columnsFilter {
			row = append(row, key.Key[c])
		}

		values = append(values, "("+strings.Join(row, ",")+")")
	}
	// when columnsFilter is empty, do not append any where clause to prevent sql syntax error
	// TODO: how it can happen to have no columnFilter when keys check at the beginning?
	where_clause := ""
	if len(columnsFilter) > 0 {
		where_clause = fmt.Sprintf("WHERE (%s) IN (%s)", strings.Join(columnsFilter, ", "), strings.Join(values, ","))
	}

	sql := fmt.Sprintf(`SELECT %s FROM %s %s;`, formattedColumns, table.Name, where_clause)
	return sqlUtil.ExecuteQueryWithResults(db, sql)
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
	values = SubstituteForeignKey(db, table, tableMap, values)
	return values
}

func substitutePrimaryKey(table schemareader.Table, row []sqlUtil.RowDataStructure) []sqlUtil.RowDataStructure {
	rowResult := make([]sqlUtil.RowDataStructure, 0)
	pkSequence := false
	if len(table.PKSequence) > 0 {
		pkSequence = true
	}
	for _, column := range row {
		if pkSequence && table.PKColumns[column.ColumnName] && len(table.PKColumns) == 1 {
			column.ColumnType = "SQL"
			column.Value = fmt.Sprintf("SELECT nextval('%s')", table.PKSequence)
			rowResult = append(rowResult, column)
		} else {
			rowResult = append(rowResult, column)
		}
	}
	return rowResult
}

func SubstituteForeignKey(db *sql.DB, table schemareader.Table, tables map[string]schemareader.Table, row []sqlUtil.RowDataStructure) []sqlUtil.RowDataStructure {
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
	formattedWhereParameters := strings.Join(whereParameters, " AND ")

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
							whereParameters = append(whereParameters, fmt.Sprintf("%s IS NULL",
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
				updateSql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s LIMIT 1`, foreignColumn, reference.TableName, strings.Join(whereParameters, " AND "))
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

	case "rhnconfiginfo":
		constraints := map[string]string{
			"rhn_confinfo_ugf_se_uq": "(username, groupname, filemode, selinux_ctx) WHERE username IS NOT NULL AND groupname IS NOT NULL AND filemode IS NOT NULL AND selinux_ctx IS NOT NULL AND symlink_target_filename_id IS NULL",
			"rhn_confinfo_ugf_uq":    "(username, groupname, filemode) WHERE username IS NOT NULL AND groupname IS NOT NULL AND filemode IS NOT NULL AND selinux_ctx IS NULL AND symlink_target_filename_id IS NULL",
			"rhn_confinfo_s_se_uq":   "(symlink_target_filename_id, selinux_ctx) WHERE username IS NULL AND groupname IS NULL AND filemode IS NULL AND selinux_ctx IS NOT NULL AND symlink_target_filename_id IS NOT NULL",
			"rhn_confinfo_s_uq":      "(symlink_target_filename_id) WHERE username IS NULL AND groupname IS NULL AND filemode IS NULL AND selinux_ctx IS NULL AND symlink_target_filename_id IS NOT NULL",
		}
		// Only username and selinux_ctx columns matter to differentiate between indexes
		columns := map[string]bool{
			"username":    false,
			"selinux_ctx": false,
		}
		// Go through all the columns first in case the columns come unordered
		for _, col := range row {
			if (col.ColumnName == "username" || col.ColumnName == "selinux_ctx") && col.Value != nil {
				columns[col.ColumnName] = true
			}
		}
		constraint = constraints["rhn_confinfo_s_uq"]
		if columns["username"] && columns["selinux_ctx"] {
			constraint = constraints["rhn_confinfo_ugf_se_uq"]
		}
		if !columns["username"] && columns["selinux_ctx"] {
			constraint = constraints["rhn_confinfo_s_se_uq"]
		}
		if columns["username"] && !columns["selinux_ctx"] {
			constraint = constraints["rhn_confinfo_ugf_uq"]
		}

	case "rhnerrata":
		// TODO rhnerrata and rhnpackageevr logic is similar, so we extract to one method on future
		var orgId interface{} = nil
		for _, field := range row {
			if strings.Compare(field.ColumnName, "org_id") == 0 {
				orgId = field.Value
				break
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

	}
	columnAssignment := formatColumnAssignment(table)
	return fmt.Sprintf("%s DO UPDATE SET %s", constraint, columnAssignment)
}

func generateClearTable(db *sql.DB, writer *bufio.Writer, table schemareader.Table, path []string,
	schemaMetadata map[string]schemareader.Table, options PrintSqlOptions) {

	// generates the delete statement for the table
	existingRecords := buildQueryToGetExistingRecords(path, table, schemaMetadata, options.CleanWhereClause)
	mainUniqueColumns := strings.Join(table.UniqueIndexes[table.MainUniqueIndexName].Columns, ",")

	cleanEmptyTable := fmt.Sprintf("\nDELETE FROM %s WHERE (%s) IN (%s);",
		table.Name, mainUniqueColumns, existingRecords)
	writer.WriteString(cleanEmptyTable + "\n")

	// repopulate all pre-existing data
	allTableRecordsSql := fmt.Sprintf("SELECT * FROM %s WHERE (%s) IN (%s);",
		table.Name, mainUniqueColumns, existingRecords)
	allTableRecords := sqlUtil.ExecuteQueryWithResults(db, allTableRecordsSql)
	for _, record := range allTableRecords {
		insertStatement := generateRowInsertStatement(db, record, table, schemaMetadata, []string{table.Name})
		writer.WriteString(insertStatement + "\n")
		//fmt.Println(insertStatement)
	}
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
						if value.ColumnType == "SQL" {
							parentsRecordsCheckList = append(parentsRecordsCheckList, fmt.Sprintf("EXISTS %s",
								formatField(value)))
						}
					}
				}
			}
		}
		whereClause := strings.Join(whereClauseList, " AND ")
		parentRecordsExistsClause := strings.Join(parentsRecordsCheckList, " AND ")

		if utils.Contains(onlyIfParentExistsTables, table.Name) {
			return fmt.Sprintf(`INSERT INTO %s (%s)	SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s) AND %s;`,
				tableName, columnNames, formatRowValue(valueFiltered), tableName, whereClause, parentRecordsExistsClause)
		}

		return fmt.Sprintf(`INSERT INTO %s (%s)	SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s);`,
			tableName, columnNames, formatRowValue(valueFiltered), tableName, whereClause)

	} else {
		onConflictFormatted := formatOnConflict(valueFiltered, table)
		return fmt.Sprintf(`INSERT INTO %s (%s)	VALUES (%s) ON CONFLICT %s;`,
			tableName, columnNames, formatRowValue(valueFiltered), onConflictFormatted)
	}

}
