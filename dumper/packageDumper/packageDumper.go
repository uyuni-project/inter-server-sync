package packageDumper

import (
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"os"
	"os/exec"
	"path/filepath"
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
		for _, rowPackage := range rows{
			path := rowPackage[pathIndex]
			source := fmt.Sprintf("%s/%s", serverDataFolder, path.Value)
			target := fmt.Sprintf("%s/%s", outputFolder, path.Value)
			error := systemCopy(source, target)
			if error != nil{
				log.Fatal().Err(error).Msg("could not Copy File: ")
			}
		}
		exportPoint = upperLimit
	}
}

func systemCopy(src, dest string) error{
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0770); err != nil {
		return err
	}

	cmd := exec.Command("cp", src, dest)
	//cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg(fmt.Sprintf("error when copy package file: %s -> %s", src, dest))
		return err
	}
	return nil
}
