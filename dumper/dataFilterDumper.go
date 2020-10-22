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

		result = followTableLinks(db, result, tableMap, make([]string, 0), tableMap["rhnchannel"], channelRow[0])
	}
	return result
}

func followTableLinks(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, path []string, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {
	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	value, ok := result[table.Name]
	if ok {
		for _, rowId := range value.Keys {
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
	if len(table.PKColumns) > 0 {
		for pkColumn, _ := range table.PKColumns {
			key[pkColumn] = formatField(row[columnIndexes[pkColumn]])
		}
	} else {
		for _, pkColumn := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
			key[pkColumn] = formatField(row[columnIndexes[pkColumn]])
		}
	}

	tableFilter, ok := result[table.Name]
	if !ok {
		tableFilter = TableFilter{TableName: table.Name, Keys: make([]TableKey, 0)}
	}
	tableFilter.Keys = append(tableFilter.Keys, TableKey{key})
	path = append(path, table.Name)

	result = followReferencesFrom(db, result, tableMap, path, table, row)
	result[table.Name] = tableFilter
	result = followReferencesTo(db, result, tableMap, path, table, row)

	return result
}

func shouldFollowReferenceByLink(path []string, table schemareader.Table, reference schemareader.Reference, referencedTable schemareader.Table) bool {

	// if we already passed by the table we don't want to follow
	for _, p := range path {
		if strings.Compare(p, referencedTable.Name) == 0 {
			return false
		}
	}
	// HACK. We should not follow links to this table
	if strings.Compare(table.Name, "rhnpackagecapability") == 0 {
		return false
	}

	// If we don't have a link from to this table we should try to use it.
	if len(referencedTable.ReferencedBy) == 0 {
		for _, ref := range referencedTable.References {
			if strings.Compare(table.Name, ref.TableName) != 0 {
				for _, p := range path {
					if strings.Compare(p, ref.TableName) == 0 {
						return false
					}
				}
			}
		}
		return true
	}
	return false
}

func followReferencesTo(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, path []string, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {
	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	for _, reference := range table.ReferencedBy {
		referencedTable, ok := tableMap[reference.TableName]
		if !ok {
			continue
		}
		if !shouldFollowReferenceByLink(path, table, reference, referencedTable) {
			continue
		}

		localColumns := make([]string, 0)
		foreignColumns := make([]string, 0)

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			localColumns = append(localColumns, localColumn)
			foreignColumns = append(foreignColumns, foreignColumn)

			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", localColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row[columnIndexes[foreignColumn]].value)
		}

		formattedColumns := strings.Join(referencedTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		rows := executeQueryWithResults(db, sql, scanParameters...)

		if len(rows) > 0 {
			for _, row := range rows {
				result = followTableLinks(db, result, tableMap, path, referencedTable, row)
			}
		}
	}

	return result
}

func followReferencesFrom(db *sql.DB, result map[string]TableFilter, tableMap map[string]schemareader.Table, path []string, table schemareader.Table, row []rowDataStructure) map[string]TableFilter {

	columnIndexes := make(map[string]int)
	for i, columnName := range table.Columns {
		columnIndexes[columnName] = i
	}

	for _, reference := range table.References {
		foreignTable, tableExist := tableMap[reference.TableName]
		if !tableExist {
			continue
		}
		passed := false
		for _, p := range path {
			if strings.Compare(p, foreignTable.Name) == 0 {
				passed = true
				break
			}
		}
		if passed {
			continue
		}

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

		if len(rows) > 0 {
			for _, row := range rows {
				result = followTableLinks(db, result, tableMap, path, foreignTable, row)
			}
		}
	}
	return result
}
