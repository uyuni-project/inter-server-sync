package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/packageDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

// TablesToClean represents Tables which needs to be cleaned in case on client side there is a record that doesn't exist anymore on master side
var tablesToClean = []string{"rhnreleasechannelmap", "rhndistchannelmap", "rhnchannelerrata", "rhnchannelpackage",
	"rhnerratapackage",
	"rhnerratafile",
	"rhnerratafilechannel", "rhnerratafilepackage", "rhnerratafilepackagesource",
	"rhnerratabuglist", "rhnerratacve", "rhnerratakeyword", "susemddata",
	"susemdkeyword",
	"suseproductchannel"}

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

func DumpChannelData(options ChannelDumperOptions) {
	var outputFolderAbs = convertAbsPath(options)
	validateExportFolder(outputFolderAbs)
	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()
	file, err := os.Create(outputFolderAbs + "/sql_statements.sql")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}

	defer file.Close()
	bufferWriter := bufio.NewWriter(file)
	defer bufferWriter.Flush()

	bufferWriter.WriteString("BEGIN;\n")
	processAndInsertProducts(db, bufferWriter)

	processAndInsertChannels(db, bufferWriter, loadChannelsToProcess(db, options), options)

	bufferWriter.WriteString("COMMIT;\n")
}

func convertAbsPath(options ChannelDumperOptions) string {
	var outputFolder = options.OutputFolder
	if filepath.IsAbs(outputFolder) {
		outputFolder, _ = filepath.Abs(outputFolder)
	} else {
		homedir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal().Msg("Couldn't determine the home directory")
			panic(err)
		}
		if strings.HasPrefix(outputFolder, "~") {
			outputFolder = strings.Replace(outputFolder, "~", homedir, -1)
		}
	}
	return outputFolder
}

func validateExportFolder(outputFolderAbs string) {
	outputFolder, err := os.Open(outputFolderAbs)
	defer outputFolder.Close()
	if os.IsNotExist(err){
		err := os.MkdirAll(outputFolderAbs, 0755)
		if err != nil {
			log.Fatal().Msg("Error creating dir")
			panic(err)
		}
		}
	outputFolder, _ = os.Open(outputFolderAbs)
	folderInfo, err := outputFolder.Stat()
	if err != nil {
		log.Fatal().Msg("Error getting folder info")
		panic(err)
	}

	if !folderInfo.IsDir(){
		log.Fatal().Msg(fmt.Sprintf("export location is not a directory: %s", outputFolderAbs))
		panic(err)
	}

	_, errEmpty := outputFolder.Readdirnames(1) // Or f.Readdir(1)
	if errEmpty != io.EOF {
		log.Fatal().Msg(fmt.Sprintf("export location is empty: %s", outputFolderAbs))
	}
}

var childChannelSql = "select label from rhnchannel " +
	"where parent_channel = (select id from rhnchannel where label = $1)"

func loadChannelsToProcess(db *sql.DB, options ChannelDumperOptions) []string {
	channels := channelsProcess{make(map[string]bool), make([]string, 0)}
	for _, singleChannel := range options.ChannelLabels{
		if _, ok := channels.channelsMap[singleChannel]; !ok{
			channels.addChannelLabel(singleChannel)
		}
	}

	for _, channelChildren := range options.ChannelWithChildrenLabels{
		if _, ok := channels.channelsMap[channelChildren]; !ok{
			channels.addChannelLabel(channelChildren)
			childrenChannels := sqlUtil.ExecuteQueryWithResults(db, childChannelSql, channelChildren)
			for _, cChannel := range childrenChannels{
				cLabel := fmt.Sprintf("%v",cChannel[0].Value)
				if _, okC := channels.channelsMap[cLabel]; !okC{
					channels.addChannelLabel(cLabel)
				}
			}

		}
	}
	return channels.channels
}

func processAndInsertProducts(db *sql.DB, writer *bufio.Writer) {
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
	log.Debug().Msg("products export done")
}

func processAndInsertChannels(db *sql.DB, writer *bufio.Writer, channels []string, options ChannelDumperOptions) {
	outputFolderAbs := convertAbsPath(options)
	log.Info().Msg(fmt.Sprintf("%d channels to process", len(channels)))

	schemaMetadata := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())
	log.Debug().Msg("channel schema metadata loaded")

	fileChannels, err := os.Create(outputFolderAbs + "/exportedChannels.txt")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}

	defer fileChannels.Close()
	bufferWriterChannels := bufio.NewWriter(fileChannels)
	defer bufferWriterChannels.Flush()

	count := 0
	for _, channelLabel := range channels {
		count++
		log.Info().Msg(fmt.Sprintf("Processing channel [%d/%d] %s", count,len(channels) ,channelLabel))
		processChannel(db, writer, options, channelLabel, schemaMetadata)
		writer.Flush()
		bufferWriterChannels.WriteString(fmt.Sprintf("%s\n", channelLabel))
	}
}

func processChannel(db *sql.DB, writer *bufio.Writer, options ChannelDumperOptions,
	channelLabel string, schemaMetadata map[string]schemareader.Table) {
	outputFolderAbs := convertAbsPath(options)
	whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
	tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnchannel"], whereFilter)
	log.Debug().Msg("finished table data crawler")

	cleanWhereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = '%s')`, channelLabel)
	printOptions := dumper.PrintSqlOptions{
		TablesToClean:            tablesToClean,
		CleanWhereClause:         cleanWhereClause,
		OnlyIfParentExistsTables: onlyIfParentExistsTables}

	dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["rhnchannel"],
		tableData, printOptions)
	log.Debug().Msg("finished print table order")

	if !options.MetadataOnly {
		log.Debug().Msg("dumping all package files")
		packageDumper.DumpPackageFiles(db, schemaMetadata, tableData, outputFolderAbs)
	}
	log.Debug().Msg("channel export finished")

}
