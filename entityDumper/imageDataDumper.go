package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/dumper/osImageDumper"
	"github.com/uyuni-project/inter-server-sync/dumper/pillarDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

// TablesToClean represents Tables which needs to be cleaned in case on client side there is a record that doesn't exist anymore on master side
var tablesToClean_images = []string{
	"suseimageinfochannel",
}

/*
    Activation keys are not exported - they are managed by uyunit formulas and/or XMLRPC/salt calls

	If correct activation key is not present, OS images, particularly saltboot images, may not finish bootstrap correctly
*/
var imagesTableNames = []string{
	// stores
	"suseImageStore",
	// profiles
	"suseImageProfile",
	"suseKiwiProfile",
	"suseDockerfileProfile",
	"rhnRegToken",
	"rhnActivationKey",
	// images
	"rhnchecksum",
	"suseImageInfo",
	"suseImageOverview",
	"suseImageInfoChannel",
	"suseImageInfoPackage",
	"suseimageinfoinstalledproduct",
	"susecveimagechannel",
	"suseImageCustomDataValue",
	// packages in image - this is needed because of custom rpm with SSL certificate
	"rhnpackageevr",
	"rhnpackagearch",
	"rhnpackagename",
	// generic table for pillars
	"suseSaltPillar",
}

var containersTableNames = []string{
	// container specific use
	"suseImageBuildHistory",
	"suseImageRepoDigest",
	// generic table for pillars
	"suseSaltPillar",
}

func ImageTableNames() []string {
	return imagesTableNames
}

func markAsExported(schema map[string]schemareader.Table, tables []string) {
	for _, table := range tables {
		tmp := schema[table]
		tmp.Export = false
		schema[table] = tmp
	}
}

func dumpImageStores(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options ImageDumperOptions, store_label string) {

	org_id := options.OrgID
	var whereFilterClause = func(table schemareader.Table) string {
		filter := fmt.Sprintf("WHERE store_type_id = (SELECT id FROM suseimagestoretype WHERE label = '%s')", store_label)
		if org_id != 0 {
			if _, ok := table.ColumnIndexes["org_id"]; ok {
				filter = fmt.Sprintf(" AND org_id = %d", org_id)
			}
		}
		return filter
	}

	processTables := make(map[string]bool)
	log.Trace().Msg("Dumping all ImageStore tables")
	writer.WriteString("-- OS Image Store\n")
	processTables = dumper.DumpReachableTablesData(db, writer, schemaMetadata, []schemareader.Table{schemaMetadata["suseimagestore"]}, whereFilterClause,
		[]string{}, processTables)
	markAsExported(schemaMetadata, []string{"suseimagestore"})
}

func dumpOSImageTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options ImageDumperOptions) {

	org_id := options.OrgID

	// Image profiles
	sqlForExistingProfiles := "SELECT profile_id FROM suseimageprofile WHERE image_type = 'kiwi'"
	if org_id != 0 {
		sqlForExistingProfiles = fmt.Sprintf("%s AND org_id = %d", sqlForExistingProfiles, org_id)
	}
	if options.StartingDate != "" {
		sqlForExistingProfiles = fmt.Sprintf("%s AND modified > '%s'::timestamp", sqlForExistingProfiles, options.StartingDate)
	}
	profiles := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingProfiles)
	if len(profiles) > 0 {
		log.Debug().Msg("Dumping ImageProfile tables")
		writer.WriteString("-- OS Image Profiles\n")
		for _, profile := range profiles {
			log.Trace().Msgf("Exporting profile id %s", profile[0].Value)
			whereClause := fmt.Sprintf("profile_id = '%s'", profile[0].Value)
			tableProfilesData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["susekiwiprofile"], whereClause, options.StartingDate)

			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["susekiwiprofile"], tableProfilesData, dumper.PrintSqlOptions{})
		}
		markAsExported(schemaMetadata, []string{"suseimageprofile"})
	} else {
		log.Info().Msg("No Kiwi profiles found to export")
	}

	// Images
	sqlForExistingImages := "SELECT id FROM suseimageinfo WHERE image_type = 'kiwi'"
	if org_id != 0 {
		sqlForExistingImages = fmt.Sprintf("%s AND org_id = %d", sqlForExistingImages, org_id)
	}
	if options.StartingDate != "" {
		sqlForExistingImages = fmt.Sprintf("%s AND modified > '%s'::timestamp", sqlForExistingImages, options.StartingDate)
	}
	images := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingImages)
	if len(images) > 0 {
		log.Debug().Msg("Dumping Image tables")
		writer.WriteString("-- OS Images\n")
		for _, image := range images {
			log.Trace().Msgf("Exporting image id %s", image[0].Value)
			whereClause := fmt.Sprintf("id = '%s'", image[0].Value)
			tableImageData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["suseimageinfo"], whereClause, options.StartingDate)
			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["suseimageinfo"], tableImageData, dumper.PrintSqlOptions{})
		}
		markAsExported(schemaMetadata, []string{"suseimageinfo"})
	}

	// Dump image pillars from database if included. Pillar files dump is handled by pillarDumper
	if _, ok := schemaMetadata["susesaltpillar"]; ok {
		log.Debug().Msg("Dumping Image pillars")
		writer.WriteString("-- OS Images pillars\n")
		pillarFilter := "category = 'images'"
		if org_id != 0 {
			pillarFilter = fmt.Sprintf("%s AND org_id = %d", pillarFilter, org_id)
		}
		if options.StartingDate != "" {
			pillarFilter = fmt.Sprintf("%s AND modified > '%s'::timestamp", pillarFilter, options.StartingDate)
		}

		pillarImageData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["susesaltpillar"], pillarFilter, options.StartingDate)
		dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["susesaltpillar"], pillarImageData, dumper.PrintSqlOptions{})
	}

	log.Info().Msg("Kiwi image profiles export done")
}

// TODO, incomplete
func dumpContainerImageTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options ImageDumperOptions) {
	startingTables := []schemareader.Table{schemaMetadata["suseimagestore"], schemaMetadata["suseimageprofile"], schemaMetadata["suseimageinfo"]}

	var whereFilterClause = func(table schemareader.Table) string {
		filterOrg := ""
		if _, ok := table.ColumnIndexes["org_id"]; ok {
			filterOrg = " WHERE org_id IS NULL"
		}
		return filterOrg
	}

	dumper.DumpAllTablesData(db, writer, schemaMetadata, startingTables, whereFilterClause, []string{"susedockerfileprofile"})
	log.Info().Msg("Dockerfile image profiles export done")
	// no data to export as they are on remote registry
}

// Main entry point
func DumpImageData(options ImageDumperOptions) {
	log.Debug().Msg("Starting image metadata dump")
	var outputFolderAbs = options.GetOutputFolderAbsPath()
	var outputFolderImagesAbs = filepath.Join(outputFolderAbs, "images")
	var outputFolderPillarAbs = filepath.Join(outputFolderAbs, "images", "pillars")
	ValidateExistingFolder(outputFolderAbs)
	ValidateExportFolder(outputFolderImagesAbs)
	ValidateExportFolder(outputFolderPillarAbs)

	// export DB data about images
	db := schemareader.GetDBconnection(options.ServerConfig)
	defer db.Close()
	file, err := os.Create(filepath.Join(outputFolderAbs, "sql_statements_images.sql"))
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating sql file")
	}

	defer file.Close()
	bufferWriter := bufio.NewWriter(file)
	defer bufferWriter.Flush()

	log.Trace().Msg("Loading table schema")
	schemaMetadata := schemareader.ReadTablesSchema(db, imagesTableNames)

	bufferWriter.WriteString("BEGIN;\n")

	if options.OSImage {
		dumpImageStores(db, bufferWriter, schemaMetadata, options, "os_image")
		dumpOSImageTables(db, bufferWriter, schemaMetadata, options)
		pillarDumper.DumpImagePillars(outputFolderPillarAbs, options.OrgID, options.ServerConfig)
		osImageDumper.DumpOsImages(outputFolderImagesAbs, options.OrgID)
	}
	if options.Containers {
		dumpImageStores(db, bufferWriter, schemaMetadata, options, "registry")
		dumpContainerImageTables(db, bufferWriter, schemaMetadata, options)
	}

	bufferWriter.WriteString("COMMIT;\n")
}
