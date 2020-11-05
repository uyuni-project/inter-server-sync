package dumper

import (
	"database/sql"
	"fmt"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() []string {
	return []string{
		// dictionaries
		"rhnarchtype",
		"rhnchecksumtype",
		"rhnpackagearch",
		"web_customer",
		"rhnchannelarch",
		"rhnerrataseverity",
		// data to transfer: products
		"rhnproductname",
		"rhnchannelproduct",
		"suseproductchannel",
		"suseproductextension",
		"suseproducts",
		"suseproductsccrepository",
		"susesccrepository",
		// data to transfer: channels
		"rhnchannel",
		//"rhnchannelcloned",
		"rhnchannelfamily",
		"rhnchannelfamilymembers",
		"rhnpublicchannelfamily",
		"rhnerrata",
		"rhnchannelerrata",
		"rhnpackagenevra",
		"rhnpackagename",  // done
		"rhnpackagegroup", // done
		"rhnsourcerpm",    // done
		"rhnpackageevr",   // done
		"rhnchecksum",     // done
		"rhnpackage",
		"rhnchannelpackage",
		"rhnerratapackage",
		"rhnpackageprovider", // catalog
		"rhnpackagekeytype",  // catalog
		"rhnpackagekey",      // catalog
		"rhnpackagekeyassociation",
		"rhnerratabuglist",
		"rhncve",
		"rhnerratacve",
		"rhnerratakeyword",
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
		"susemddata",
		"susemdkeyword",
	}
}

func DumpeChannelData(db *sql.DB, channelLabels []string, outputFolder string) DataDumper {

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
	PrintTableDataOrdered(db, outputFolder, schemaMetadata, schemaMetadata["rhnchannel"], tableData)
	return tableData
}
