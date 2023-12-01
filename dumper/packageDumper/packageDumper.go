// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package packageDumper

import (
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"time"

	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

var serverDataFolder = "/var/spacewalk"

func DumpPackageFiles(db *sql.DB, schemaMetadata map[string]schemareader.Table, data dumper.DataDumper, outputFolder string) {

	packageKeysData := data.TableData["rhnpackage"]
	table := schemaMetadata[packageKeysData.TableName]
	pathIndex := table.ColumnIndexes["path"]

	totalPackages := len(packageKeysData.Keys)
	log.Debug().Msgf("Total package files to copy: %d", totalPackages)

	exportedpackages := 0
	processing := true

	if log.Debug().Enabled() {
		go func() {
			count := 0
			for {
				if !processing {
					break
				}
				time.Sleep(30 * time.Second)
				log.Debug().Msgf("#count: %d -- #exportedPackageFiles: #%d of %d",
					count, exportedpackages, totalPackages)
				count++
			}
		}()
	}

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
				log.Panic().Err(error).Msg("could not Copy File")
			}
			exportedpackages++
		}
		exportPoint = upperLimit
	}
	processing = false
}
