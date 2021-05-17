package packageDumper

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

var serverDataFolder = "/var/spacewalk"

func DumpPackageFiles(db *sql.DB, schemaMetadata map[string]schemareader.Table, data dumper.DataDumper, outputFolder string) {

	packageKeysData := data.TableData["rhnpackage"]
	table := schemaMetadata[packageKeysData.TableName]
	pathIndex := table.ColumnIndexes["path"]

	exportPoint := 0
	batchSize := 500
	for len(packageKeysData.Keys) > exportPoint {
		upperLimit := exportPoint + batchSize
		if upperLimit > len(packageKeysData.Keys) {
			upperLimit = len(packageKeysData.Keys)
		}
		rows := dumper.GetRowsFromKeys(db, table, packageKeysData.Keys[exportPoint:upperLimit])
		for _, rowPackage := range rows {
			path := rowPackage[pathIndex]
			source := fmt.Sprintf("%s/%s", serverDataFolder, path.Value)
			target := fmt.Sprintf("%s/%s", outputFolder, path.Value)
			_, error := dumper.Copy(source, target)
			if error != nil {
				log.Panic("could not Copy File: ", error)
			}
		}
		exportPoint = upperLimit
	}
}
