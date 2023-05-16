package entityDumper

import (
	"bufio"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

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

// Activation keys are not exported - they are managed by uyuni formulas and/or XMLRPC/salt calls
// If correct activation key is not present, OS images, particularly saltboot images, may not finish bootstrap correctly
var imagesTableNames = []string{
	// stores
	"suseImageStore",
	"suseImageStoreType",
	"suseCredentials",
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

func markAsExported(schema map[string]schemareader.Table, tables []string) {
	for _, table := range tables {
		tmp := schema[table]
		tmp.Export = false
		schema[table] = tmp
	}
}

func markAsUnexported(schema map[string]schemareader.Table, tables []string) {
	for _, table := range tables {
		tmp := schema[table]
		tmp.Export = true
		schema[table] = tmp
	}
}

func isColumnInTable(schema map[string]schemareader.Table, table string, column string) bool {
	columns := schema[table].Columns
	for _, c := range columns {
		if strings.Compare(c, column) == 0 {
			return true
		}
	}
	return false
}

func dumpImageStores(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options DumperOptions, store_label string) {

	sqlForExistingStores := fmt.Sprintf(
		"SELECT sis.id from suseimagestore AS sis JOIN suseimagestoretype AS sist ON sis.store_type_id = sist.id WHERE sist.label = '%s'", store_label)
	for _, org := range options.Orgs {
		sqlForExistingStores = fmt.Sprintf("%s AND sis.org_id = %d", sqlForExistingStores, org)
	}
	if options.StartingDate != "" {
		sqlForExistingStores = fmt.Sprintf("%s AND sis.modified > '%s'::timestamp", sqlForExistingStores, options.StartingDate)
	}
	stores := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingStores)
	if len(stores) > 0 {
		log.Debug().Msgf("Dumping ImageStores tables for label %s", store_label)
		writer.WriteString(fmt.Sprintf("-- %s Image Stores\n", store_label))
		for _, store := range stores {
			log.Trace().Msgf("Exporting store id %s", store[0].Value)
			whereClause := fmt.Sprintf("id = '%s'", store[0].Value)
			tableProfilesData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["suseimagestore"], whereClause, options.StartingDate)

			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["suseimagestore"], tableProfilesData, dumper.PrintSqlOptions{})
		}
		// Mark tables as exported so they are not transitively exported by profiles
		markAsExported(schemaMetadata, []string{"suseimagestore"})
	} else {
		log.Info().Msg("No image stores found to export")
	}
}

/*
*

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
		// Mark tables as exported so they are not transitively exported by images
		markAsExported(schemaMetadata, []string{"suseimageprofile"})
	} else {
		log.Info().Msg("No Kiwi profiles found to export")
	}

	// Images
	needExtraExport := false
	sqlForExistingImages := "SELECT id FROM suseimageinfo WHERE image_type = 'kiwi'"
	if isColumnInTable(schemaMetadata, "suseimageinfo", "built") {
		// For 4.3 and newer export only succesfuly built images
		sqlForExistingImages = fmt.Sprintf("%s AND built = 'Y'", sqlForExistingImages)
	}
	for _, org := range options.Orgs {
		sqlForExistingImages = fmt.Sprintf("%s AND org_id = %d", sqlForExistingImages, org)
	}
	if options.StartingDate != "" {
		sqlForExistingImages = fmt.Sprintf("%s AND modified > '%s'::timestamp", sqlForExistingImages, options.StartingDate)
	}
	images := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingImages)
	if len(images) > 0 {
		dumperOptions := dumper.PrintSqlOptions{
			OnlyIfParentExistsTables: []string{"suseimageinfochannel"},
		}
		log.Debug().Msg("Dumping Image tables")
		writer.WriteString("-- OS Images\n")
		for _, image := range images {
			log.Trace().Msgf("Exporting image id %s", image[0].Value)
			whereClause := fmt.Sprintf("id = '%s'", image[0].Value)
			tableImageData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["suseimageinfo"], whereClause, options.StartingDate)
			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["suseimageinfo"], tableImageData, dumperOptions)
			// Check if pillars are already in database
			if _, ok := tableImageData.TableData["susesaltpillar"]; ok && !options.MetadataOnly {
				// pillars in database, files must be as well
				// export all metadata about images, but skip linked suseimageinfo
				markAsExported(schemaMetadata, []string{"suseimageinfo"})
				whereClauseImageFiles := fmt.Sprintf("image_info_id = '%s'", image[0].Value)
				tableImageFilesData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["suseimagefile"],
					whereClauseImageFiles, options.StartingDate)
				dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["suseimagefile"],
					tableImageFilesData, dumper.PrintSqlOptions{})
				// find all local (not-external) image files for the image and export their files
				sqlForExistingLocalImageFiles := fmt.Sprintf("SELECT file, org_id FROM suseimagefile AS sif JOIN suseimageinfo AS sii "+
					"ON sif.image_info_id = sii.id WHERE sii.id = '%s' AND external = 'N'", image[0].Value)
				imageFiles := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingLocalImageFiles)
				for _, imageFile := range imageFiles {
					// source is taken from basedir + org + filename from db
					// output should be base abs dir + org + filename from db
					file := (imageFile[0].Value).(string)
					org := fmt.Sprintf("%s", imageFile[1].Value)
					source := osImageDumper.GetImagePathForImage(file, org)
					target := osImageDumper.GetImagePathForImage(file, org, outputFolderImagesAbs)
					osImageDumper.DumpOsImage(target, source)
				}
				// we marked this as exported for image files, now we need to unexport for the rest of the images
				markAsUnexported(schemaMetadata, []string{"suseimageinfo"})
			} else {
				// pillars and thus image files are not in database, need extra export step
				needExtraExport = true
			}
		}
	}

	log.Info().Msg("Kiwi image export done")
	return needExtraExport
}

func dumpContainerImageTables(db *sql.DB, writer *bufio.Writer, schemaMetadata map[string]schemareader.Table, options DumperOptions) {

	// Image profiles
	sqlForExistingProfiles := "SELECT profile_id FROM suseimageprofile WHERE image_type = 'dockerfile'"
	for _, org := range options.Orgs {
		sqlForExistingProfiles = fmt.Sprintf("%s AND org_id = %d", sqlForExistingProfiles, org)
	}
	if options.StartingDate != "" {
		sqlForExistingProfiles = fmt.Sprintf("%s AND modified > '%s'::timestamp", sqlForExistingProfiles, options.StartingDate)
	}
	profiles := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingProfiles)
	if len(profiles) > 0 {
		log.Debug().Msg("Dumping ImageProfile tables")
		writer.WriteString("-- Dockerfile Profiles\n")
		for _, profile := range profiles {
			log.Trace().Msgf("Exporting profile id %s", profile[0].Value)
			whereClause := fmt.Sprintf("profile_id = '%s'", profile[0].Value)
			tableProfilesData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["susedockerfileprofile"], whereClause, options.StartingDate)

			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["susedockerfileprofile"], tableProfilesData, dumper.PrintSqlOptions{})
		}
		markAsExported(schemaMetadata, []string{"suseimageprofile"})
	} else {
		log.Info().Msg("No profiles found to export")
	}

	// Images
	sqlForExistingImages := "SELECT id FROM suseimageinfo WHERE image_type = 'dockerfile'"
	if isColumnInTable(schemaMetadata, "suseimageinfo", "built") {
		// For 4.3 and newer export only succesfuly built images
		sqlForExistingImages = fmt.Sprintf("%s AND built = 'Y'", sqlForExistingImages)
	}
	for _, org := range options.Orgs {
		sqlForExistingImages = fmt.Sprintf("%s AND org_id = %d", sqlForExistingImages, org)
	}
	if options.StartingDate != "" {
		sqlForExistingImages = fmt.Sprintf("%s AND modified > '%s'::timestamp", sqlForExistingImages, options.StartingDate)
	}
	images := sqlUtil.ExecuteQueryWithResults(db, sqlForExistingImages)
	if len(images) > 0 {
		log.Debug().Msg("Dumping Image tables")
		writer.WriteString("-- Dockerfile Images\n")
		for _, image := range images {
			log.Trace().Msgf("Exporting image id %s", image[0].Value)
			whereClause := fmt.Sprintf("id = '%s'", image[0].Value)
			tableImageData := dumper.DataCrawler(db, schemaMetadata, schemaMetadata["suseimageinfo"], whereClause, options.StartingDate)
			dumper.PrintTableDataOrdered(db, writer, schemaMetadata, schemaMetadata["suseimageinfo"], tableImageData, dumper.PrintSqlOptions{})
		}
	}

	log.Info().Msg("Dockerfile image export done")
}

// Main entry point
func dumpImageData(db *sql.DB, writer *bufio.Writer, options DumperOptions) {
	log.Debug().Msg("Starting image metadata dump")
	var outputFolderAbs = options.GetOutputFolderAbsPath()

	// export DB data about images
	log.Trace().Msg("Loading table schema")
	schemaMetadata := schemareader.ReadTablesSchema(db, imagesTableNames)

	if options.OSImages {
		var outputFolderImagesAbs = filepath.Join(outputFolderAbs, "images")
		ValidateExportFolder(outputFolderImagesAbs)
		dumpImageStores(db, writer, schemaMetadata, options, "os_image")
		if dumpOSImageTables(db, writer, schemaMetadata, options, outputFolderImagesAbs) {
			var outputFolderPillarAbs = filepath.Join(outputFolderAbs, "images", "pillars")
			ValidateExportFolder(outputFolderPillarAbs)
			pillarDumper.DumpImagePillars(outputFolderPillarAbs, options.Orgs, options.ServerConfig)
			if !options.MetadataOnly {
				osImageDumper.DumpOsImages(outputFolderImagesAbs, options.Orgs)
			}
		}
		// This is needed for containers to be able to export their respective tables
		markAsUnexported(schemaMetadata, []string{"suseimagestore", "suseimageprofile"})
	}
	if options.Containers {
		dumpImageStores(db, writer, schemaMetadata, options, "registry")
		dumpContainerImageTables(db, writer, schemaMetadata, options)
	}
}
