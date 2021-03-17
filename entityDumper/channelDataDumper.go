package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/uyuni-project/inter-server-sync/dumper"
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

func DumpChannelData(db *sql.DB, channelLabels []string, outputFolder string) []dumper.DataDumper {

	file, err := os.Create(outputFolder + "/sql_statements.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bufferWritter := bufio.NewWriter(file)
	defer bufferWritter.Flush()

	bufferWritter.WriteString("BEGIN;\n")
	processAndInsertProducts(db, bufferWritter)
	channelsResult := processAndInsertChannels(db, channelLabels, bufferWritter)
	bufferWritter.WriteString("COMMIT;\n")
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

func processAndInsertChannels(db *sql.DB, channelLabels []string, writter *bufio.Writer) []dumper.DataDumper {
	schemaMetadata := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())
	tableDumper := make([]dumper.DataDumper, 0)
	for _, channelLabel := range channelLabels {
		log.Printf("Processing...%s", channelLabel)
		whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
		tableData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["rhnchannel"],whereFilter )
		cleanWhereClause := fmt.Sprintf(`WHERE rhnchannel.id = (SELECT id FROM rhnchannel WHERE label = '%s')`, channelLabel)
		dumper.PrintTableDataOrdered(db, writter, schemaMetadata, schemaMetadata["rhnchannel"], tableData, cleanWhereClause, tablesToClean, onlyIfParentExistsTables)
		tableDumper = append(tableDumper, tableData)
	}
	return tableDumper

}