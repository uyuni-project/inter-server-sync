package dumper

import (
	"database/sql"
	"fmt"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"strings"
)

// dataCrawler will go through all the elements in the initialDataSet an extract related data
// for all tables presented in the schemaMetadata by following foreign keys and references to the table row
// The result will be a structure containing ID of each row which should be exported per table
func dataCrawler(db *sql.DB, schemaMetadata map[string]schemareader.Table, initialDataSet []processItem) DataDumper {

	result := DataDumper{make(map[string]TableDump, 0), make(map[string]bool)}

	itemsToProcess := initialDataSet

IterateItemsLoop:
	for len(itemsToProcess) > 0 {

		itemToProcess := itemsToProcess[0]
		itemsToProcess = itemsToProcess[1:]

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

		itemsToProcess = append(itemsToProcess, followReferencesFrom(db, schemaMetadata, table, itemToProcess)...)
		itemsToProcess = append(itemsToProcess, followReferencesTo(db, schemaMetadata, table, itemToProcess)...)

	}
	return result
}

func generateKeyIdToMap(data map[string]string) string {
	keyValuesList := make([]string, 0)
	for _, value := range data {
		keyValuesList = append(keyValuesList, value)
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

func followReferencesFrom(db *sql.DB, schemaMetadata map[string]schemareader.Table, table schemareader.Table, row processItem) []processItem {
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
			scanParameters = append(scanParameters, row.row[table.ColumnIndexes[localColumn]].value)
		}

		formattedColumns := strings.Join(foreignTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		followRows := executeQueryWithResults(db, sql, scanParameters...)

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

func shouldFollowReferenceToLink(path []string, currentTable schemareader.Table, referencedTable schemareader.Table) bool {
	// if we already passed by the referencedTable we don't want to follow
	for _, p := range path {
		if strings.Compare(p, referencedTable.Name) == 0 {
			return false
		}
	}

	forcedNavegations := map[string] []string {
		"rhnchannelfamily": {"rhnpublicchannelfamily"},
		"rhnchannel": {"susemddata", "suseproductchannel"},
		"suseproducts": {"suseproductextension", "suseproductsccrepository"},
		"rhnpackageevr": {"rhnpackagenevra"},
	}

	if tableNavegation, ok := forcedNavegations[currentTable.Name]; ok {
		for _, targetNavegationTable := range tableNavegation{
			if strings.Compare(targetNavegationTable, referencedTable.Name) == 0{
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

func followReferencesTo(db *sql.DB, schemaMetadata map[string]schemareader.Table, table schemareader.Table, row processItem) []processItem {
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
			scanParameters = append(scanParameters, row.row[table.ColumnIndexes[foreignColumn]].value)
		}

		formattedColumns := strings.Join(referencedTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		followRows := executeQueryWithResults(db, sql, scanParameters...)

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
