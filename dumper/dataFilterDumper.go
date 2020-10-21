package dumper

import (
	"database/sql"
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
	"strconv"
	"strings"
)

func DumpTableFilter(db *sql.DB, tables []schemareader.Table, ids []int) map[string]TableFilter {
	result := make(map[string]TableFilter, 0)

	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	for _, channelId := range ids {
		channelFilter, ok := result["rhnchannel"]
		if !ok {
			channelFilter = TableFilter{TableName: "rhnchannel", WhereClauses: make([]TableKey, 0)}
		}
		whereFilter := fmt.Sprintf("id = %d", channelId)
		channelFilter.WhereClauses = append(channelFilter.WhereClauses, TableKey{map[string]string{"id": strconv.Itoa(channelId)}})
		result["rhnchannel"] = channelFilter

		sql := fmt.Sprintf(`SELECT * FROM rhnchannel where %s ;`, whereFilter)
		channelRow := executeQueryWithResults(db, sql)

		result = followTableLinks(db, result, tableMap, tableMap["rhnchannel"], channelRow[0])
		//mergeFilters(result, followedLinks)

	}

	return result
}

func mergeFilters(mergeTo map[string]TableFilter, mergeFrom map[string]TableFilter) map[string]TableFilter {
	result := mergeTo
	for key, fromValue := range mergeFrom {
		val, ok := result[key]
		if ok {
			result[key] = TableFilter{TableName: key, WhereClauses: append(val.WhereClauses, fromValue.WhereClauses...)}
		} else {
			result[key] = mergeFrom[key]
		}
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
		for _, rowId := range value.WhereClauses {
			for columnName, rowIdColumnValue := range rowId.key {
				if strings.Compare(rowIdColumnValue, formatField(row[columnIndexes[columnName]])) != 0 {
					return result
				}
			}
		}
	}

	result = followReferencesFrom(db, result, tableMap, table, row)
	result = followReferencesTo(db, result, tableMap, table, row)

	fmt.Printf("%s \n %s \n\n", table.Name, result)

	return result
}

func followReferencesTo(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {
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
		tableFilter, ok := result[foreignTable.Name]
		if !ok {
			tableFilter = TableFilter{TableName: foreignTable.Name, WhereClauses: make([]TableKey, 0)}
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
			tableFilter.WhereClauses = append(tableFilter.WhereClauses, TableKey{filterWhere})
			result[foreignTable.Name] = tableFilter

			for _, row := range rows {
				result = followTableLinks(db, result, tableMap, foreignTable, row)
			}
		}
	}
	return result
}
