package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"os"
	"strings"
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

func loadConfigsToProcess(db *sql.DB, options DumperOptions) []string {
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

func processConfigs(db *sql.DB, writer *bufio.Writer, labels []string, options DumperOptions) {
	log.Info().Msg(fmt.Sprintf("%d configuration channels to process", len(labels)))
	schemaMetadata := schemareader.ReadTablesSchema(db, ConfigTableNames())
	log.Debug().Msg("channel schema metadata loaded")
	configLabels, err := os.Create(options.GetOutputFolderAbsPath() + "/exportedConfigs.txt")
	if err != nil {
		log.Panic().Err(err).Msg("error creating exportedConfigChannel file")
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
	schemaMetadata map[string]schemareader.Table, options DumperOptions) {
	whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
	tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnconfigchannel"], whereFilter, options.StartingDate)
	log.Debug().Msg("finished table data crawler")

	cleanWhereClause := fmt.Sprintf(`WHERE rhnconfigchannel.id = (SELECT id FROM rhnconfigchannel WHERE label = '%s')`, channelLabel)
	printOptions := dumper.PrintSqlOptions{
		TablesToClean:            tablesToClean,
		CleanWhereClause:         cleanWhereClause,
		OnlyIfParentExistsTables: onlyIfParentExistsTables,
		PostOrderCallback:        createPostOrderCallback(),
	}

	dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["rhnconfigchannel"],
		tableData, printOptions)
	log.Debug().Msg("finished print table order")
	log.Debug().Msg("config channel export finished")
}

func createPostOrderCallback() dumper.Callback {
	return func(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
		table schemareader.Table, data dumper.DataDumper) {

		tableData, dataOK := data.TableData[table.Name]
		if strings.Compare(table.Name, "rhnconfigfile") == 0 {
			if dataOK {
				exportPoint := 0
				batch := 100
				for len(tableData.Keys) > exportPoint {
					upperLimit := exportPoint + batch
					if upperLimit > len(tableData.Keys) {
						upperLimit = len(tableData.Keys)
					}
					rows := dumper.GetRowsFromKeys(db, table, tableData.Keys[exportPoint:upperLimit])
					for _, rowValue := range rows {
						rowValue = dumper.SubstituteForeignKey(db, table, schemaMetadata, rowValue)
						updateString := genUpdateForReference(rowValue)
						writer.WriteString(updateString + "\n")
					}
					exportPoint = upperLimit
				}
			}
		}
	}
}

func genUpdateForReference(value []sqlUtil.RowDataStructure) string {
	var updateString string
	var latestConfigRevisionId, configFileNameId, configChannelId interface{}
	for _, field := range value {
		if strings.Compare(field.ColumnName, "latest_config_revision_id") == 0 {
			latestConfigRevisionId = field.Value
		}
		if strings.Compare(field.ColumnName, "config_file_name_id") == 0 {
			configFileNameId = field.Value
		}
		if strings.Compare(field.ColumnName, "config_channel_id") == 0 {
			configChannelId = field.Value
		}
	}
	updateString = fmt.Sprintf("update rhnconfigfile set latest_config_revision_id = (%s) where config_file_name_id = (%s) and config_channel_id = (%s);", latestConfigRevisionId, configFileNameId, configChannelId)
	return updateString
}
