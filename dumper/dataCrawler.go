package dumper

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

// DataCrawler will go through all the elements in the initialDataSet an extract related data
// for all tables presented in the schemaMetadata by following foreign keys and references to the table row
// The result will be a structure containing ID of each row which should be exported per table
func DataCrawler(db *sql.DB, schemaMetadata map[string]schemareader.Table, startTable schemareader.Table, startQueryFilter string, startingDate string) DataDumper {

	result := DataDumper{make(map[string]TableDump, 0), make(map[string]bool)}

	itemsToProcess := initialDataSet(db, startTable, startQueryFilter)

IterateItemsLoop:
	for len(itemsToProcess) > 0 {

		// LIFO instead of FIFO improves performance
		itemToProcess := itemsToProcess[len(itemsToProcess)-1]
		itemsToProcess = itemsToProcess[0 : len(itemsToProcess)-1]

		table, tableExists := schemaMetadata[itemToProcess.tableName]
		if !tableExists {
			continue IterateItemsLoop
		}

		keyColumnData := extractRowKeyData(table, itemToProcess)
		keyIdToMap := generateKeyIdToMap(keyColumnData)

		resultTableValues, resultExists := result.TableData[table.Name]
		if resultExists {
			_, rowProcessed := resultTableValues.KeyMap[keyIdToMap]
			if rowProcessed {
				continue IterateItemsLoop
			}
		} else {
			resultTableValues = TableDump{TableName: table.Name, KeyMap: make(map[string]bool), Keys: make([]TableKey, 0)}
		}
		resultTableValues.KeyMap[keyIdToMap] = true
		resultTableValues.Keys = append(resultTableValues.Keys, TableKey{keyColumnData})

		result.TableData[table.Name] = resultTableValues
		_, okPath := result.Paths[strings.Join(itemToProcess.path, ",")]
		if !okPath {
			result.Paths[strings.Join(itemToProcess.path, ",")] = true
		}

		newItems := append(followReferencesTo(db, schemaMetadata, table, itemToProcess, startingDate),
			followReferencesFrom(db, schemaMetadata, table, itemToProcess, startingDate)...)
		itemsToProcess = append(itemsToProcess, newItems...)

	}
	return result
}

func initialDataSet(db *sql.DB, startTable schemareader.Table, whereFilter string) []processItem {
	sql := fmt.Sprintf(`SELECT * FROM %s where %s ;`, startTable.Name, whereFilter)
	rows := sqlUtil.ExecuteQueryWithResults(db, sql)
	initialDataSet := make([]processItem, 0)
	for _, row := range rows {
		initialDataSet = append(initialDataSet, processItem{startTable.Name, row, []string{startTable.Name}})
	}
	return initialDataSet
}

func generateKeyIdToMap(data map[string]string) string {
	keyValuesList := make([]string, 0)
	for _, value := range data {
		valueStr := fmt.Sprintf("%s", value)
		keyValuesList = append(keyValuesList, valueStr)
	}
	return strings.Join(keyValuesList, "$$")
}

func extractRowKeyData(table schemareader.Table, itemToProcess processItem) map[string]string {
	keyColumnData := make(map[string]string)
	if len(table.PKColumns) > 0 {
		for pkColumn, _ := range table.PKColumns {
			keyColumnData[pkColumn] = formatField(itemToProcess.row[table.ColumnIndexes[pkColumn]])
		}
	} else {
		for _, pkColumn := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
			keyColumnData[pkColumn] = formatField(itemToProcess.row[table.ColumnIndexes[pkColumn]])
		}
	}
	return keyColumnData
}

func followReferencesFrom(db *sql.DB, schemaMetadata map[string]schemareader.Table, table schemareader.Table, row processItem, startingDate string) []processItem {
	result := make([]processItem, 0)

	for _, reference := range table.References {
		foreignTable, ok := schemaMetadata[reference.TableName]
		if !ok {
			continue
		}
		targetTableVisited := false
		for _, p := range row.path {
			if strings.Compare(p, foreignTable.Name) == 0 {
				targetTableVisited = true
				break
			}
		}
		if targetTableVisited {
			continue
		}

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", foreignColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row.row[table.ColumnIndexes[localColumn]].Value)
		}

		if startingDate != "" && (reference.TableName == "rhnchannelerrata" || reference.TableName == "rhnchannelpackage" || reference.TableName == "susemddata") {
			whereParameters = append(whereParameters, fmt.Sprintf("%s >= '$%d'::timestamp", "modified", len(whereParameters)+1))
			scanParameters = append(scanParameters, startingDate)
		}

		formattedColumns := strings.Join(foreignTable.Columns, ", ")
		formattedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formattedWhereParameters)
		followRows := sqlUtil.ExecuteQueryWithResults(db, sql, scanParameters...)

		if len(followRows) > 0 {
			for _, followRow := range followRows {
				newPath := make([]string, 0)
				newPath = append(newPath, row.path...)
				newPath = append(newPath, foreignTable.Name)
				result = append(result, processItem{foreignTable.Name, followRow, newPath})
			}
		}
	}
	return result
}

func shouldFollowToLinkPreOrder(path []string, currentTable schemareader.Table, referencedTable schemareader.Table) bool {
	forbiddenNavigations := map[string][]string{
		"rhnconfigfile": {"rhnconfigrevision"},
	}

	if tableNavigation, ok := forbiddenNavigations[currentTable.Name]; ok {
		for _, targetNavigationTable := range tableNavigation {
			if strings.Compare(targetNavigationTable, referencedTable.Name) == 0 {
				return false
			}
		}
	}
	
	return true
}

func shouldFollowReferenceToLink(path []string, currentTable schemareader.Table, referencedTable schemareader.Table) bool {
	// if we already passed by the referencedTable we don't want to follow
	for _, p := range path {
		if strings.Compare(p, referencedTable.Name) == 0 {
			return false
		}
	}

	forcedNavegations := map[string][]string{
		"rhnchannelfamily": {"rhnpublicchannelfamily"},
		"rhnchannel":       {"susemddata", "suseproductchannel", "rhnreleasechannelmap", "rhndistchannelmap"},
		"suseproducts":     {"suseproductextension", "suseproductsccrepository"},
		"rhnpackageevr":    {"rhnpackagenevra"},
		"rhnerrata":        {"rhnerratafile"},
		"rhnerratafile":    {"rhnerratafilechannel"},
		"rhnconfigchannel": {"rhnconfigfile"},
		"rhnconfigfile":    {"rhnconfigrevision"},
	}

	if tableNavigation, ok := forcedNavegations[currentTable.Name]; ok {
		for _, targetNavigationTable := range tableNavigation {
			if strings.Compare(targetNavigationTable, referencedTable.Name) == 0 {
				return true
			}
		}
	}

	// If referencedTable don't have any link to it, we should try to use it
	// Also in the referencedTable it the currentTable is the linking table dominant, by comparing is name
	if len(referencedTable.ReferencedBy) == 0 && strings.HasPrefix(referencedTable.Name, currentTable.Name) {
		for _, ref := range referencedTable.References {
			//In the referencedTable we will go through all the references
			// ignoring the ones to the currentTable.
			// And see if we have already passed (part of path) in one of the reference tables of referencedTable
			// If we already passed, we should not follow this path, because we have been already here
			if strings.Compare(currentTable.Name, ref.TableName) != 0 {
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

func followReferencesTo(db *sql.DB, schemaMetadata map[string]schemareader.Table, table schemareader.Table, row processItem, startingDate string) []processItem {
	result := make([]processItem, 0)

	for _, reference := range table.ReferencedBy {
		referencedTable, ok := schemaMetadata[reference.TableName]
		if !ok {
			continue
		}
		if !shouldFollowReferenceToLink(row.path, table, referencedTable) {
			continue
		}

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", localColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row.row[table.ColumnIndexes[foreignColumn]].Value)
		}

		if startingDate != "" && (reference.TableName == "rhnchannelerrata" || reference.TableName == "rhnchannelpackage" || reference.TableName == "susemddata") {
			whereParameters = append(whereParameters, fmt.Sprintf("%s >= $%d::timestamp", "modified", len(whereParameters)+1))
			scanParameters = append(scanParameters, startingDate)
		}

		formattedColumns := strings.Join(referencedTable.Columns, ", ")
		formattedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formattedWhereParameters)
		followRows := sqlUtil.ExecuteQueryWithResults(db, sql, scanParameters...)

		if len(followRows) > 0 {
			for _, followRow := range followRows {
				newPath := make([]string, 0)
				newPath = append(newPath, row.path...)
				newPath = append(newPath, referencedTable.Name)
				result = append(result, processItem{referencedTable.Name, followRow, newPath})
			}
		}
	}
	return result
}
