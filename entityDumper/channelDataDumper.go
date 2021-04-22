package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/packageDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"os"
)

// TablesToClean represents Tables which needs to be cleaned in case on client side there is a record that doesn't exist anymore on master side
var tablesToClean = []string{"rhnreleasechannelmap", "rhndistchannelmap", "rhnchannelerrata", "rhnchannelpackage", "rhnerratapackage", "rhnerratafile",
	"rhnerratafilechannel", "rhnerratafilepackage", "rhnerratafilepackagesource", "rhnerratabuglist", "rhnerratacve", "rhnerratakeyword", "susemddata", "susemdkeyword",
	"suseproductchannel"}

// onlyIfParentExistsTables represents Tables for which only records needs to be insterted only if parent record exists
var onlyIfParentExistsTables = []string{"rhnchannelcloned", "rhnerratacloned", "suseproductchannel"}

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() []string {
	return []string{
		// software channel data tables
		"rhnchannel",
		"rhnchannelcloned", // add only if there are corresponding rows in rhnchannel
		"suseproductchannel",       // add only if there are corresponding rows in rhnchannel // clean
		"rhnproductname",
		"rhnchannelproduct",
		"rhnreleasechannelmap", // clean
		"rhndistchannelmap",    // clean
		"rhnchannelcomps",
		"rhnchannelfamily",
		"rhnchannelfamilymembers",
		"rhnpublicchannelfamily",
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

func DumpChannelData(db *sql.DB, channelLabels []string, outputFolder string, metadataOnly bool) []dumper.DataDumper {

	file, err := os.Create(outputFolder + "/sql_statements.sql")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}
	defer file.Close()
	bufferWritter := bufio.NewWriter(file)
	defer bufferWritter.Flush()

	bufferWritter.WriteString("BEGIN;\n")
	processAndInsertProducts(db, bufferWritter)

	schemaMetadataChannel := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())
	channelsResult := processAndInsertChannels(db, schemaMetadataChannel, channelLabels, bufferWritter)
	bufferWritter.WriteString("COMMIT;\n")
	// should we copy the files only in the end? or should we copy on each channel iteration?
	if !metadataOnly{
		exportChannelsPackageFiles(db,schemaMetadataChannel,  channelsResult, outputFolder)
	}
	return channelsResult
}

func processAndInsertProducts(db *sql.DB, writter *bufio.Writer) {
	schemaMetadata := schemareader.ReadTablesSchema(db, ProductsTableNames())
	startingTables := []schemareader.Table{schemaMetadata["suseproducts"]}

	var whereFilterClause = func(table schemareader.Table) string {
		filterOrg := ""
		if _, ok := table.ColumnIndexes["org_id"]; ok {
			filterOrg = " where org_id is null"
		}
		return filterOrg
	}

	dumper.DumpAllTablesData(db, writter, schemaMetadata, startingTables, whereFilterClause, onlyIfParentExistsTables)
}

func processAndInsertChannels(db *sql.DB, schemaMetadata map[string]schemareader.Table, channelLabels []string, writter *bufio.Writer) []dumper.DataDumper {
	tableDumper := make([]dumper.DataDumper, 0)
	for _, channelLabel := range channelLabels {
		log.Printf("Processing...%s", channelLabel)
		whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
		tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnchannel"], whereFilter )
		cleanWhereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = '%s')`, channelLabel)
		dumper.PrintTableDataOrdered(db, writter, schemaMetadata, schemaMetadata["rhnchannel"],
			tableData, dumper.PrintSqlOptions{TablesToClean: tablesToClean,
				CleanWhereClause: cleanWhereClause,
				OnlyIfParentExistsTables: onlyIfParentExistsTables })
		tableDumper = append(tableDumper, tableData)
	}
	// should we save in memory all datadumper information in memory?
	// this will not scale in large setups and when exporting several channels at same time.
	return tableDumper
}

func exportChannelsPackageFiles(db *sql.DB, schemaMetadata map[string]schemareader.Table, data []dumper.DataDumper, outputFolder string) {
	for _, tableData := range data {
		packageDumper.DumpPackageFiles(db, schemaMetadata, tableData, outputFolder)
	}
}