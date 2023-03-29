package entityDumper

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/utils"
	"io"
	"os"
)

func setOptionsByConfig(options *DumperOptions) {
	var config ExportConfig

	if options.Config == "" {
		return
	}

	configFile, err := os.Open(utils.GetAbsPath(options.Config))
	if err != nil {
		log.Panic().Err(err).Msg("failed to open config file")
	}
	defer func(configFile *os.File) {
		err := configFile.Close()
		if err != nil {
			log.Warn().Err(err).Msg("failed to close config file")
		}
	}(configFile)

	configBytes, err := io.ReadAll(configFile)
	if err != nil {
		log.Panic().Err(err).Msg("failed to read config file")
	}

	if err = json.Unmarshal(configBytes, &config); err != nil {
		log.Panic().Err(err).Msg("failed to unmarshal config file")
	}

	if options.ServerConfig == "/etc/rhn/rhn.conf" && config.ServerConfig != "" {
		options.ServerConfig = config.ServerConfig
	}

	if options.OutputFolder == "." && config.OutputDir != "" {
		options.OutputFolder = config.OutputDir
	}

	if len(options.ChannelLabels) == 0 {
		options.ChannelLabels = config.Channels
	}

	if len(options.ChannelWithChildrenLabels) == 0 {
		options.ChannelWithChildrenLabels = config.ChannelWithChildren
	}

	if len(options.ConfigLabels) == 0 {
		options.ConfigLabels = config.ConfigChannels
	}

	if config.MetadataOnly {
		options.MetadataOnly = config.MetadataOnly
	}

	if options.StartingDate == "" {
		options.StartingDate = config.StartingDate
	}

	if len(options.Orgs) == 0 {
		options.Orgs = config.Orgs
	}

	if config.IncludeImages {
		options.OSImages = config.IncludeImages
	}

	if config.IncludeContainers {
		options.Containers = config.IncludeContainers
	}
}

func DumpAllEntities(options DumperOptions) {
	setOptionsByConfig(&options)

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
