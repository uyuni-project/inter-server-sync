package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

//"rhnarchtype",
//"rhnchecksumtype",
//"rhnpackagearch",
//"web_customer",
//"rhnchannelarch",
//"rhnerrataseverity",
//"rhncompstype",
//"rhnerratafiletype",
//"rhnpackageprovider",
//"rhnpackagekeytype",
//"rhnpackagekey",

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() []string {
	return []string{
		// product data tables
		"suseproducts",             // clean
		"suseproductchannel",       // add only if there are corresponding rows in rhnchannel // clean
		"suseproductextension",     // clean
		"suseproductsccrepository", // clean
		"susesccrepository",        // clean
		"suseupgradepath",          // clean

		// software channel data tables
		"rhnchannel",
		// FIXME This table needs a special treatement to check if channels exists. Inser to into.. select .. were
		//"rhnchannelcloned", // add only if there are corresponding rows in rhnchannel
		"rhnproductname",
		"rhnchannelproduct",
		"rhnreleasechannelmap", // clean
		"rhndistchannelmap",    // clean
		"rhnchannelcomps",
		"rhnchannelfamily",
		"rhnchannelfamilymembers",
		"rhnpublicchannelfamily",
		"rhnerrata",
		// FIXME This table needs a special treatement to check if channels exists. Inser to into.. select .. were
		//"rhnerratacloned", // add only if there are corresponding rows in rhnerrata
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

func DumpeChannelData(db *sql.DB, channelLabels []string, outputFolder string) DataDumper {

	file, err := os.Create(outputFolder + "/sql_statements.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bufferWritter := bufio.NewWriter(file)
	defer bufferWritter.Flush()

	bufferWritter.WriteString("BEGIN;\n")
	result := processAndInsertChannels(db, channelLabels, bufferWritter)
	bufferWritter.WriteString("COMMIT;\n")
	return result
}

func processAndInsertChannels(db *sql.DB, channelLabels []string, writter *bufio.Writer) DataDumper{
	schemaMetadata := schemareader.ReadTablesSchema(db, SoftwareChannelTableNames())

	initalDataSet := make([]processItem, 0)
	for _, channelLabel := range channelLabels {
		whereFilter := fmt.Sprintf("label = '%s'", channelLabel)
		sql := fmt.Sprintf(`SELECT * FROM rhnchannel where %s ;`, whereFilter)
		rows := executeQueryWithResults(db, sql)
		for _, row := range rows {
			initalDataSet = append(initalDataSet, processItem{schemaMetadata["rhnchannel"].Name, row, []string{"rhnchannel"}})
		}

	}
	tableData := dataCrawler(db, schemaMetadata, initalDataSet)
	PrintTableDataOrdered(db, writter, schemaMetadata, schemaMetadata["rhnchannel"], tableData)
	return tableData
}