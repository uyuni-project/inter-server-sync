package entityDumper

import (
	"bufio"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"os"
)

func DumpAllEntities(options DumperOptions) {
	var outputFolderAbs = options.GetOutputFolderAbsPath()
	validateExportFolder(outputFolderAbs)
	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()

	channelsExport := loadChannelsToProcess(db, options)

	file, err := os.OpenFile(outputFolderAbs+"/sql_statements.sql", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}

	defer file.Close()
	bufferWriter := bufio.NewWriter(file)
	defer bufferWriter.Flush()

	bufferWriter.WriteString("BEGIN;\n")
	if len(options.ChannelLabels) > 0 {
		processAndInsertProducts(db, bufferWriter)
		processAndInsertChannels(db, bufferWriter, channelsExport, options)
	}
	if len(options.ConfigLabels) > 0 {
		processConfigs(db, bufferWriter, loadConfigsToProcess(db, options), options)
	}

	bufferWriter.WriteString("COMMIT;\n")
}
