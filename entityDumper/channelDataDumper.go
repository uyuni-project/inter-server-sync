package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/packageDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
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
		"rhnchannelcloned",   // add only if there are corresponding rows in rhnchannel
		"suseproductchannel", // add only if there are corresponding rows in rhnchannel // clean
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

func DumpChannelData(options ChannelDumperOptions) {

	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()

	file, err := os.Create(options.OutputFolder + "/sql_statements.sql")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating sql file")
		panic(err)
	}

	defer file.Close()
	bufferWriter := bufio.NewWriter(file)
	defer bufferWriter.Flush()

	bufferWriter.WriteString("BEGIN;\n")
	processAndInsertProducts(db, bufferWriter)

	processAndInsertChannels(db, bufferWriter, options)
	bufferWriter.WriteString("COMMIT;\n")
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

func processAndInsertChannels(db *sql.DB, writer *bufio.Writer, options ChannelDumperOptions) {

	// check for duplicated channels andnot export
	// add option to export "with-childs"

	schemaMetadata := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())
	log.Debug().Msg("channel schema metadata loaded")

	for _, channelLabel := range options.ChannelLabels {
		processChannel(db, writer, options, channelLabel, schemaMetadata)
		writer.Flush()
	}
}

func processChannel(db *sql.DB, writer *bufio.Writer, options ChannelDumperOptions,
	channelLabel string, schemaMetadata map[string]schemareader.Table) {

	log.Debug().Msg(fmt.Sprintf("Processing...%s", channelLabel))

	whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
	tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnchannel"], whereFilter)
	log.Debug().Msg("finished table data crawler")

	cleanWhereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = '%s')`, channelLabel)
	// memory problem is on pint data ordered.
	// after data crawller it was 1.5 G, During dump is on 2.5..
	dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["rhnchannel"],
		tableData, dumper.PrintSqlOptions{TablesToClean: tablesToClean,
			CleanWhereClause:         cleanWhereClause,
			OnlyIfParentExistsTables: onlyIfParentExistsTables})
	log.Debug().Msg("finished print table order")

	if !options.MetadataOnly {
		log.Debug().Msg("dumping all package files")
		packageDumper.DumpPackageFiles(db, schemaMetadata, tableData, options.OutputFolder)
	}
	log.Debug().Msg("channel export finished")

}
