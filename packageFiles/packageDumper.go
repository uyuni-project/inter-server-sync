package packageFiles

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"io"
	"log"
	"os"
	"path/filepath"
)

func PrintTableDataOrdered(db *sql.DB, schemaMetadata map[string]schemareader.Table, data dumper.DataDumper, outputFolder string) {

	file, err := os.Create(outputFolder + "/copyFiles.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bufferWritter := bufio.NewWriter(file)
	defer bufferWritter.Flush()

	packageKeysData := data.TableData["rhnpackage"]
	table := schemaMetadata[packageKeysData.TableName]

	rows := dumper.GetRowsFromKeys(db, schemaMetadata, packageKeysData)

	pathIndex := table.ColumnIndexes["path"]
	for _, rowPackage := range rows{
		path := rowPackage[pathIndex]
		source := fmt.Sprintf("%s/%s", "/var/spacewalk", path.Value)
		target := fmt.Sprintf("%s/%s", outputFolder, path.Value)
		bufferWritter.WriteString(fmt.Sprintf("'%s'---->'%s'\n", source, target))
		_, error := copy(source, target)
		if error != nil{
			log.Fatal("could not Copy File: ", error)
		}
	}

}


func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func create(p string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0770); err != nil {
		return nil, err
	}
	return os.Create(p)
}