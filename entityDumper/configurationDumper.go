package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/packageDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"os"
)

func ConfigTableNames() []string {
	return []string{
		"rhnconfigfile",
		"rhnconfigfilename",
		"rhnconfigrevision",
		"rhnconfigcontent",
		"rhnconfigchannel",
		"rhnconfigfilestate",
		"rhnregtokenconfigchannels",
		"rhnserverconfigchannel",
		"rhnsnapshotconfigchannel",
		"susestaterevisionconfigchannel",
		"rhnconfiginfo",
		"rhnconfigfilefailure",
		"rhnchecksum",
		"rhnchecksumtype",
		"web_contact",
	}
}

func DumpConfigs(options ChannelDumperOptions) {
	var outputFolderAbs = options.GetOutputFolderAbsPath()
	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()
	//file, err := os.OpenFile(outputFolderAbs + "/sql_statements.sql", os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600)
	file, err := os.Create(outputFolderAbs + "/configurations.sql")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}
	defer file.Close()
	bufferWriter := bufio.NewWriter(file)
	defer bufferWriter.Flush()

	bufferWriter.WriteString("BEGIN;\n")
	processConfigs(db, bufferWriter, loadConfigsToProcess(db, options), options)

	bufferWriter.WriteString("COMMIT;\n")
}

func loadConfigsToProcess(db *sql.DB, options ChannelDumperOptions) []string {
	labels := channelsProcess{make(map[string]bool), make([]string, 0)}
	for _, singleChannel := range options.ConfigLabels {
		if _, ok := labels.channelsMap[singleChannel]; !ok {
			labels.addChannelLabel(singleChannel)
		}
	}

	for _, channelChildren := range options.ChannelWithChildrenLabels {
		if _, ok := labels.channelsMap[channelChildren]; !ok {
			labels.addChannelLabel(channelChildren)
			childrenChannels := sqlUtil.ExecuteQueryWithResults(db, childChannelSql, channelChildren)
			for _, cChannel := range childrenChannels {
				cLabel := fmt.Sprintf("%v", cChannel[0].Value)
				if _, okC := labels.channelsMap[cLabel]; !okC {
					labels.addChannelLabel(cLabel)
				}
			}

		}
	}
	return labels.channels
}

func processConfigs(db *sql.DB, writer *bufio.Writer, labels []string, options ChannelDumperOptions) {
	log.Info().Msg(fmt.Sprintf("%d channels to process", len(labels)))
	schemaMetadata := schemareader.ReadTablesSchema(db, ConfigTableNames())
	log.Debug().Msg("channel schema metadata loaded")
	configLabels, err := os.Create(options.GetOutputFolderAbsPath() + "/exportedConfigs.sql")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating exportedConfigChannel file")
		panic(err)
	}
	defer configLabels.Close()
	bufferWriterChannels := bufio.NewWriter(configLabels)
	defer bufferWriterChannels.Flush()

	count := 0
	for _, l := range labels {
		count++
		log.Info().Msg(fmt.Sprintf("Processing channel [%d/%d] %s", count, len(labels), l))
		processConfigChannel(db, writer, l, schemaMetadata, options)
		writer.Flush()
		bufferWriterChannels.WriteString(fmt.Sprintf("%s\n", l))
	}

}

func processConfigChannel(db *sql.DB, writer *bufio.Writer, channelLabel string,
	schemaMetadata map[string]schemareader.Table, options ChannelDumperOptions) {
	whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
	tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnconfigchannel"], whereFilter)
	log.Debug().Msg("finished table data crawler")

	cleanWhereClause := fmt.Sprintf(`WHERE rhnconfigchannel.id = (SELECT id FROM rhnconfigchannel WHERE label = '%s')`, channelLabel)
	printOptions := dumper.PrintSqlOptions{
		TablesToClean:            tablesToClean,
		CleanWhereClause:         cleanWhereClause,
		OnlyIfParentExistsTables: onlyIfParentExistsTables}

	dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["rhnconfigchannel"],
		tableData, printOptions)
	log.Debug().Msg("finished print table order")
	if !options.MetadataOnly {
		log.Debug().Msg("dumping all package files")
		packageDumper.DumpPackageFiles(db, schemaMetadata, tableData, options.GetOutputFolderAbsPath())
	}
	log.Debug().Msg("config channel export finished")
}
