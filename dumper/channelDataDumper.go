package dumper

import (
	"database/sql"
	"fmt"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() map[string]bool {
	return map[string]bool{
		// dictionaries
		"rhnarchtype": true,
		"rhnchecksumtype": true,
		"rhnpackagearch": true,
		"web_customer": false,
		"rhnchannelarch": true,
		"rhnerrataseverity": true,
		// data to transfer: products
		"rhnproductname": true,
		"rhnchannelproduct": true,
		"suseproductchannel": true,
		"suseproductextension": true,
		"suseproducts": true,
		"suseproductsccrepository": true,
		"susesccrepository": true,
		// data to transfer: channels
		"rhnchannel": true,
		//"rhnchannelcloned": true,
		"rhnchannelfamily": true,
		"rhnchannelfamilymembers": true,
		"rhnpublicchannelfamily": true,
		"rhnerrata": true,
		"rhnchannelerrata": true,
		"rhnpackagenevra": true,
		"rhnpackagename": true,  // done
		"rhnpackagegroup": true, // done
		"rhnsourcerpm": true,    // done
		"rhnpackageevr": true,   // done
		"rhnchecksum": true,     // done
		"rhnpackage": true,
		"rhnchannelpackage": true,
		"rhnerratapackage": true,
		"rhnpackageprovider": false, // catalog
		"rhnpackagekeytype": false,  // catalog
		"rhnpackagekey": false,      // catalog
		"rhnpackagekeyassociation": true,
		"rhnerratabuglist": true,
		"rhncve": true,
		"rhnerratacve": true,
		"rhnerratakeyword": true,
		"rhnpackagecapability": true,
		"rhnpackagebreaks": true,
		"rhnpackagechangelogdata": true,
		"rhnpackagechangelogrec": true,
		"rhnpackageconflicts": true,
		"rhnpackageenhances": true,
		"rhnpackagefile": true,
		"rhnpackageobsoletes": true,
		"rhnpackagepredepends": true,
		"rhnpackageprovides": true,
		"rhnpackagerecommends": true,
		"rhnpackagerequires": true,
		"rhnpackagesource": true,
		"rhnpackagesuggests": true,
		"rhnpackagesupplements": true,
		"susemddata": true,
		"susemdkeyword": true,
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
