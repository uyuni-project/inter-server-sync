package dumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/uyuni-project/inter-server-sync/schemareader"
)

// SoftwareChannelTableNames is the list of names of tables relevant for exporting software channels
func SoftwareChannelTableNames() map[string]bool {
	return map[string]bool{
		// dictionaries
		"rhnarchtype":        false,
		"rhnchecksumtype":    false,
		"rhnpackagearch":     false,
		"web_customer":       false,
		"rhnchannelarch":     false,
		"rhnerrataseverity":  false,
		"rhncompstype":       false,
		"rhnerratafiletype":  false,
		"rhnpackageprovider": false,
		"rhnpackagekeytype":  false,
		"rhnpackagekey":      false,

		// product data tables
		"suseproducts":             true,
		"suseproductchannel":       true, // add only if there are corresponding rows in rhnchannel
		"suseproductextension":     true,
		"suseproductsccrepository": true,
		"susesccrepository":        true,
		"suseupgradepath":          true,

		// software channel data tables
		"rhnchannel":                 true,
		"rhnchannelcloned":           true, // add only if there are corresponding rows in rhnchannel
		"rhnproductname":             true,
		"rhnchannelproduct":          true,
		"rhnreleasechannelmap":       true,
		"rhndistchannelmap":          true,
		"rhnchannelcomps":            true,
		"rhnchannelfamilymembers":    true,
		"rhnpublicchannelfamily":     true,
		"rhnerrata":                  true,
		"rhnerratacloned":            true, // add only if there are corresponding rows in rhnerrata
		"rhnchannelerrata":           true,
		"rhnpackagenevra":            true,
		"rhnpackagename":             true,
		"rhnpackagegroup":            true,
		"rhnpackageevr":              true,
		"rhnchecksum":                true,
		"rhnpackage":                 true,
		"rhnchannelpackage":          true,
		"rhnerratapackage":           true,
		"rhnerratafile":              true,
		"rhnerratafilechannel":       true,
		"rhnerratafilepackage":       true,
		"rhnerratafilepackagesource": true,
		"rhnpackagekeyassociation":   true,
		"rhnerratabuglist":           true,
		"rhncve":                     true,
		"rhnerratacve":               true,
		"rhnerratakeyword":           true,
		"rhnpackagecapability":       true,
		"rhnpackagebreaks":           true,
		"rhnpackagechangelogdata":    true,
		"rhnpackagechangelogrec":     true,
		"rhnpackageconflicts":        true,
		"rhnpackageenhances":         true,
		"rhnpackagefile":             true,
		"rhnpackageobsoletes":        true,
		"rhnpackagepredepends":       true,
		"rhnpackageprovides":         true,
		"rhnpackagerecommends":       true,
		"rhnpackagerequires":         true,
		"rhnsourcerpm":               true,
		"rhnpackagesource":           true,
		"rhnpackagesuggests":         true,
		"rhnpackagesupplements":      true,
		"susemddata":                 true,
		"susemdkeyword":              true,
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