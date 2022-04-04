package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
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
	// images
	"rhnchecksum",
	"suseImageFile",
	"suseImageInfo",
	"suseImageInfoChannel",
	"suseImageInfoPackage",
	"suseimageinfoinstalledproduct",
	"suseImageOverview",
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

func dumpImageStores(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options DumperOptions, store_label string) {

	var whereFilterClause = func(table schemareader.Table) string {
		filter := fmt.Sprintf("WHERE store_type_id = (SELECT id FROM suseimagestoretype WHERE label = '%s')", store_label)
		if _, ok := table.ColumnIndexes["org_id"]; ok {
			for _, org := range options.Orgs {
				filter = fmt.Sprintf(" AND org_id = %d", org)
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

/**
  Dump OS image tables, return true if additional data (pillars, images) need to be also dumped
*/
func dumpOSImageTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table,
	options DumperOptions, outputFolderImagesAbs string) bool {

	// Image profiles
	sqlForExistingProfiles := "SELECT profile_id FROM suseimageprofile WHERE image_type = 'kiwi'"
	for _, org := range options.Orgs {
		sqlForExistingProfiles = fmt.Sprintf("%s AND org_id = %d", sqlForExistingProfiles, org)
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
	needExtraExport := false
	sqlForExistingImages := "SELECT id FROM suseimageinfo WHERE image_type = 'kiwi'"
	for _, org := range options.Orgs {
		sqlForExistingImages = fmt.Sprintf("%s AND org_id = %d", sqlForExistingImages, org)
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
			// Check if pillars are already in database
			if _, ok := tableImageData.TableData["susesaltpillar"]; ok && !options.MetadataOnly {
				// pillars in database, files must be as well
				// find all image files for the image and export them
				sqlForExistingImageFiles := fmt.Sprintf("SELECT file, org_id FROM suseimagefile AS sif JOIN suseimageinfo AS sii "+
					"ON sif.image_info_id = sii.id WHERE sii.id = '%s' AND external = 'N'", image[0].Value)
				imageFiles := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingImageFiles)
				for _, imageFile := range imageFiles {
					// source is taken from basedir + org + filename from db
					// output should be base abs dir + org + filename from db
					file := (imageFile[0].Value).(string)
					org := fmt.Sprintf("%s", imageFile[1].Value)
					source := osImageDumper.GetImagePathForImage(file, org)
					target := osImageDumper.GetImagePathForImage(file, org, outputFolderImagesAbs)
					osImageDumper.DumpOsImage(target, source)
				}

			} else {
				// pillars and thus image files are not in database, need extra export step
				needExtraExport = true
			}
		}
		markAsExported(schemaMetadata, []string{"suseimageinfo"})
	}

	log.Info().Msg("Kiwi image export done")
	return needExtraExport
}

// TODO, incomplete
func dumpContainerImageTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options DumperOptions) {
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
func dumpImageData(db *sql.DB, writer *bufio.Writer, options DumperOptions) {
	log.Debug().Msg("Starting image metadata dump")
	var outputFolderAbs = options.GetOutputFolderAbsPath()
	var outputFolderImagesAbs = filepath.Join(outputFolderAbs, "images")
	var outputFolderPillarAbs = filepath.Join(outputFolderAbs, "images", "pillars")
	ValidateExportFolder(outputFolderImagesAbs)
	ValidateExportFolder(outputFolderPillarAbs)

	// export DB data about images
	log.Trace().Msg("Loading table schema")
	schemaMetadata := schemareader.ReadTablesSchema(db, imagesTableNames)

	if options.OSImages {
		dumpImageStores(db, writer, schemaMetadata, options, "os_image")
		if dumpOSImageTables(db, writer, schemaMetadata, options, outputFolderImagesAbs) {
			pillarDumper.DumpImagePillars(outputFolderPillarAbs, options.Orgs, options.ServerConfig)
			if !options.MetadataOnly {
				osImageDumper.DumpOsImages(outputFolderImagesAbs, options.Orgs)
			}
		}
	}
	if options.Containers {
		dumpImageStores(db, writer, schemaMetadata, options, "registry")
		dumpContainerImageTables(db, writer, schemaMetadata, options)
	}
}
