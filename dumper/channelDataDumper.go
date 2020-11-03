package dumper

import (
	"database/sql"
	"fmt"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

func readTableNames() []string {
	return []string{
		// dictionaries
		"rhnproductname",
		"rhnchannelproduct",
		"rhnarchtype",
		"rhnchecksumtype",
		"rhnpackagearch",
		"web_customer",
		"rhnchannelarch",
		"rhnerrataseverity", // catalog
		// data to transfer
		"rhnchannel",
		"rhnchannelfamily",
		"rhnchannelfamilymembers",
		"rhnerrata",
		"rhnchannelerrata",
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
	}
}

func DumpeChannelData(db *sql.DB, channelLabels []string, outputFolder string) DataDumper {

	schemaMetadata := schemareader.ReadTablesSchema(db, readTableNames())

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
	PrintTableDataOrdered(db, outputFolder, schemaMetadata, tableData)
	return tableData
}
