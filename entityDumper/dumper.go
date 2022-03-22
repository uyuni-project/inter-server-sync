package entityDumper

import (
	"bufio"
	"compress/gzip"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

func DumpAllEntities(options DumperOptions) {
	var outputFolderAbs = options.GetOutputFolderAbsPath()
	validateExportFolder(outputFolderAbs)

	file, err := os.OpenFile(outputFolderAbs+"/sql_statements.sql.gz", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Panic().Err(err).Msg("error creating sql file")
	}
	defer file.Close()

	gzipFile := gzip.NewWriter(file)
	defer gzipFile.Close()

	bufferWriter := bufio.NewWriterSize(gzipFile, 32768)
	defer bufferWriter.Flush()

	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()
	bufferWriter.WriteString("BEGIN;\n")
	if len(options.ChannelLabels) > 0 || len(options.ChannelWithChildrenLabels) > 0 {
		processAndInsertProducts(db, bufferWriter)
		processAndInsertChannels(db, bufferWriter, options)
	}
	if len(options.ConfigLabels) > 0 {
		processConfigs(db, bufferWriter, options)
	}

	if options.OSImages || options.Containers {
		dumpImageData(db, bufferWriter, options)
	}

	bufferWriter.WriteString("COMMIT;\n")
}
