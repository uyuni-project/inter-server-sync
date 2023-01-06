package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/packageDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"github.com/uyuni-project/inter-server-sync/utils"
)

// TablesToClean represents Tables which needs to be cleaned in case on client side there is a record that doesn't exist anymore on master side
var tablesToClean = []string{"rhnreleasechannelmap", "rhndistchannelmap", "rhnchannelerrata", "rhnchannelpackage",
	"rhnerratapackage",
	"rhnerratafile",
	"rhnerratafilechannel", "rhnerratafilepackage", "rhnerratafilepackagesource",
	"rhnerratabuglist", "rhnerratacve",
	"rhnerratakeyword",
	"susemddata", "suseproductchannel", "rhnchannelcloned",
	"rhnpackageextratag"}

// onlyIfParentExistsTables represents Tables for which only records needs to be insterted only if parent record exists
var onlyIfParentExistsTables = []string{"rhnchannelcloned", "rhnerratacloned", "suseproductchannel"}

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() []string {
	return []string{
		// software channel data tables
		"rhnchannel",
		"rhnchannelcloned",   // add only if there are corresponding rows in rhnchannel
		"suseproductchannel", // add only if there are corresponding rows in rhnchannel // clean
		"rhnproductname",
		"rhnchannelproduct",
		"rhnreleasechannelmap", // clean
		"rhndistchannelmap",    // clean
		"rhnchannelcomps",
		"rhnchannelfamilymembers",
		"rhnerrata",
		"rhnerratacloned",  // add only if there are corresponding rows in rhnerrata
		"rhnchannelerrata", // clean
		"rhnpackagenevra",
		"rhnpackagename",
		"rhnpackagegroup",
		"rhnpackageevr",
		"rhnchecksum",
		"rhnpackage",
		"rhnchannelpackage",          // clean
		"rhnerratapackage",           // clean
		"rhnerratafile",              // clean
		"rhnerratafilechannel",       // clean
		"rhnerratafilepackage",       // clean
		"rhnerratafilepackagesource", // clean
		"rhnpackagekeyassociation",
		"rhnpackagekey",
		"rhnerratabuglist", // clean
		"rhncve",
		"rhnerratacve",     // clean
		"rhnerratakeyword", // clean
		"rhnpackagecapability",
		"rhnpackagebreaks",
		"rhnpackagechangelogdata",
		"rhnpackagechangelogrec",
		"rhnpackageconflicts",
		"rhnpackageenhances",
		"rhnpackageextratag",
		"rhnpackageextratagkey",
		"rhnpackagefile",
		"rhnpackageobsoletes",
		"rhnpackagepredepends",
		"rhnpackageprovides",
		"rhnpackagerecommends",
		"rhnpackagerequires",
		"rhnsourcerpm",
		"rhnpackagesource",
		"rhnpackagesuggests",
		"rhnpackagesupplements",
		"susemddata",    // clean
		"susemdkeyword", // clean
	}
}

func ProductsTableNames() []string {
	return []string{
		// product data tables
		"suseproducts",             // clean
		"suseproductextension",     // clean
		"suseproductsccrepository", // clean
		"susesccrepository",        // clean
		"suseupgradepath",          // clean
		// product data tables
		"rhnchannelfamily",
		"rhnpublicchannelfamily",
	}
}

func validateExportFolder(outputFolderAbs string) {
	err := utils.FolderExists(outputFolderAbs)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(outputFolderAbs, 0755)
			if err != nil {
				log.Fatal().Err(err).Msg("Error creating directory")
			}
		} else {
			log.Fatal().Err(err).Msg("Error getting output folder")
		}
	}
	outputFolder, _ := os.Open(outputFolderAbs)
	defer outputFolder.Close()
	_, errEmpty := outputFolder.Readdirnames(1) // Or f.Readdir(1)
	if errEmpty != io.EOF {
		log.Fatal().Msg(fmt.Sprintf("export location is not empty: %s", outputFolderAbs))
	}
}

var childChannelSql = "select label from rhnchannel " +
	"where parent_channel = (select id from rhnchannel where label = $1)"

var singleChannelSql = "select label from rhnchannel " +
	"where label = $1"

func loadChannelsToProcess(db *sql.DB, options DumperOptions) []string {
	log.Trace().Msg("Loading channel list")
	channels := channelsProcess{make(map[string]bool), make([]string, 0)}
	for _, singleChannel := range options.ChannelLabels {
		if _, ok := channels.channelsMap[singleChannel]; !ok {
			dbChannel := sqlUtil.ExecuteQueryWithResults(db, singleChannelSql, singleChannel)
			if len(dbChannel) == 0 {
				log.Fatal().Msgf("Channel not found: %s", singleChannel)
			}
			channels.addChannelLabel(singleChannel)
		}
	}

	for _, channelChildren := range options.ChannelWithChildrenLabels {
		if _, ok := channels.channelsMap[channelChildren]; !ok {
			dbChannel := sqlUtil.ExecuteQueryWithResults(db, singleChannelSql, channelChildren)
			if len(dbChannel) == 0 {
				log.Fatal().Msgf("Channel not found: %s", channelChildren)
			}
			channels.addChannelLabel(channelChildren)
			childrenChannels := sqlUtil.ExecuteQueryWithResults(db, childChannelSql, channelChildren)
			for _, cChannel := range childrenChannels {
				cLabel := fmt.Sprintf("%v", cChannel[0].Value)
				if _, okC := channels.channelsMap[cLabel]; !okC {
					channels.addChannelLabel(cLabel)
				}
			}

		}
	}
	log.Debug().Msgf("Channels to export: %s", strings.Join(channels.channels, ","))
	return channels.channels
}

func processAndInsertProducts(db *sql.DB, writer *bufio.Writer) {
	log.Trace().Msg("Processing product tables")
	schemaMetadata := schemareader.ReadTablesSchema(db, ProductsTableNames())
	startingTables := []schemareader.Table{schemaMetadata["suseproducts"]}

	var whereFilterClause = func(table schemareader.Table) string {
		filterOrg := ""
		if _, ok := table.ColumnIndexes["org_id"]; ok {
			filterOrg = " where org_id is null"
		}
		return filterOrg
	}

	dumper.DumpAllTablesData(db, writer, schemaMetadata, startingTables, whereFilterClause, onlyIfParentExistsTables)
	writer.WriteString("-- end of product tables")
	writer.WriteString("\n")
	log.Debug().Msg("products export done")
}

func processAndInsertChannels(db *sql.DB, writer *bufio.Writer, options DumperOptions) {

	channels := loadChannelsToProcess(db, options)
	log.Info().Msg(fmt.Sprintf("%d channels to process", len(channels)))

	schemaMetadata := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())
	log.Debug().Msg("channel schema metadata loaded")

	fileChannels, err := os.Create(options.GetOutputFolderAbsPath() + "/exportedChannels.txt")
	if err != nil {
		log.Panic().Err(err).Msg("error creating sql file")
	}

	defer fileChannels.Close()
	bufferWriterChannels := bufio.NewWriter(fileChannels)
	defer bufferWriterChannels.Flush()

	count := 0
	for _, channelLabel := range channels {
		count++
		log.Info().Msg(fmt.Sprintf("Processing channel [%d/%d] %s", count, len(channels), channelLabel))
		processChannel(db, writer, channelLabel, schemaMetadata, options)
		writer.Flush()
		bufferWriterChannels.WriteString(fmt.Sprintf("%s\n", channelLabel))
	}
}

func processChannel(db *sql.DB, writer *bufio.Writer, channelLabel string,
	schemaMetadata map[string]schemareader.Table, options DumperOptions) {
	whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
	tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnchannel"], whereFilter, options.StartingDate)

	if log.Debug().Enabled() {
		totalRows := 0
		for _, value := range tableData.TableData {
			totalRows = totalRows + len(value.KeyMap)
		}
		log.Debug().Msgf("finished table data crawler. Total database rows to export: %d", totalRows)
	}

	cleanWhereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = '%s')`, channelLabel)
	printOptions := dumper.PrintSqlOptions{
		TablesToClean:            tablesToClean,
		CleanWhereClause:         cleanWhereClause,
		OnlyIfParentExistsTables: onlyIfParentExistsTables}

	dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["rhnchannel"],
		tableData, printOptions)
	log.Debug().Msg("finished print table order")

	generateCacheCalculation(channelLabel, writer)

	if !options.MetadataOnly {
		log.Debug().Msg("dumping all package files")
		packageDumper.DumpPackageFiles(db, schemaMetadata, tableData, options.GetOutputFolderAbsPath())
	}
	log.Debug().Msg("channel export finished")

}

func generateCacheCalculation(channelLabel string, writer *bufio.Writer) {
	// need to update channel modify since it's use to run repo metadata generation
	updateChannelModifyDate := fmt.Sprintf("update rhnchannel set modified = current_timestamp where label = '%s';", channelLabel)
	writer.WriteString(updateChannelModifyDate + "\n")

	// force system updates packages/patches for system using the channel
	serverErrataCache := fmt.Sprintf("select rhn_channel.update_needed_cache((select id from rhnchannel where label ='%s'));", channelLabel)
	writer.WriteString(serverErrataCache + "\n")

	// refreshes the package newest page
	channelNewPackages := fmt.Sprintf("select rhn_channel.refresh_newest_package((select id from rhnchannel where label ='%s'), 'inter-server-sync');", channelLabel)
	writer.WriteString(channelNewPackages + "\n")

	// generates the repository metadata on disk
	repoMetadata := fmt.Sprintf(`
		INSERT INTO rhnRepoRegenQueue
		(id, channel_label, client, reason, force, bypass_filters, next_action, created, modified)
		VALUES (null, '%s', 'inter server sync v2', 'channel sync', 'N', 'N', current_timestamp, current_timestamp, current_timestamp);
	`, channelLabel)
	writer.WriteString(repoMetadata + "\n")
}
