package dumper

import (
	"database/sql"
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
	"strings"
)

func DumpTableFilter(db *sql.DB, tables []schemareader.Table, ids []int) map[string]TableFilter {
	result := make(map[string]TableFilter, 0)

	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	for _, channelId := range ids {
		whereFilter := fmt.Sprintf("id = %d", channelId)
		sql := fmt.Sprintf(`SELECT * FROM rhnchannel where %s ;`, whereFilter)
		channelRow := executeQueryWithResults(db, sql)

		result = followTableLinks(db, result, tableMap, tableMap["rhnchannel"], channelRow[0])
	}

	return result
}

func followTableLinks(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {
	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	value, ok := result[table.Name]
	if ok {
		for _, rowId := range value.keys {
			equalKey := true
			for columnName, rowIdColumnValue := range rowId.key {
				if strings.Compare(rowIdColumnValue, formatField(row[columnIndexes[columnName]])) != 0 {
					// ID already processed nothing to do
					equalKey = false
					break
				}
			}
			if equalKey {
				return result
			}
		}
	}

	key := make(map[string]string)
	for pkColumn, _ := range table.PKColumns {
		key[pkColumn] = formatField(row[columnIndexes[pkColumn]])
	}

	tableFilter, ok := result[table.Name]
	if !ok {
		tableFilter = TableFilter{TableName: table.Name, keys: make([]TableKey, 0)}
	}
	tableFilter.keys = append(tableFilter.keys, TableKey{key})
	result[table.Name] = tableFilter

	result = followReferencesFrom(db, result, tableMap, table, row)
	result = followReferencesTo(db, result, tableMap, table, row)
	fmt.Printf("%s \n %s \n\n", table.Name, result)

	return result
}

func followReferencesTo(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {

	//for _, reference := range table.ReferencedBy {
	//	referencedTable, ok := tableMap[reference.TableName]
	//	if ! ok {
	//		continue
	//	}
	//	for referenceColumn, localColumn := range reference.ColumnMapping
	//	// prepare an sql query to load data from
	//}

	return result
}

func followReferencesFrom(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {

	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	for _, reference := range table.References {
		foreignTable, tableExist := tableMap[reference.TableName]
		if !tableExist {
			continue

		}

		//foreignMainUniqueColumns := foreignTable.UniqueIndexes[foreignTable.MainUniqueIndexName].Columns
		localColumns := make([]string, 0)
		foreignColumns := make([]string, 0)

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		filterWhere := make(map[string]string)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			localColumns = append(localColumns, localColumn)
			foreignColumns = append(foreignColumns, foreignColumn)

			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", foreignColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row[columnIndexes[localColumn]].value)
			filterWhere[foreignColumn] = formatField(row[columnIndexes[localColumn]])
		}

		formattedColumns := strings.Join(foreignTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		rows := executeQueryWithResults(db, sql, scanParameters...)

		if len(rows) > 0 {
			for _, row := range rows {
				result = followTableLinks(db, result, tableMap, foreignTable, row)
			}
		}
	}
	return result
}
