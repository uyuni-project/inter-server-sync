package cmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/dumper/pillarDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
	"github.com/uyuni-project/inter-server-sync/xmlrpc"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data to server",
	Run:   runImport,
}

var importDir string
var xmlRpcUser string
var xmlRpcPassword string

func init() {

	importCmd.Flags().StringVar(&importDir, "importDir", ".", "Location import data from")
	importCmd.Flags().StringVar(&xmlRpcUser, "xmlRpcUser", "admin", "A username to access the XML-RPC Api")
	importCmd.Flags().StringVar(&xmlRpcPassword, "xmlRpcPassword", "admin", "A password to access the XML-RPC Api")
	importCmd.Args = cobra.NoArgs

	rootCmd.AddCommand(importCmd)
}

func runImport(cmd *cobra.Command, args []string) {
	absImportDir := utils.GetAbsPath(importDir)
	log.Info().Msg(fmt.Sprintf("starting import from dir %s", absImportDir))
	fversion, fproduct := getImportVersionProduct(absImportDir)
	sversion, sproduct := utils.GetCurrentServerVersion(serverConfig)
	if fversion != sversion || fproduct != sproduct {
		log.Panic().Msgf("Wrong version detected. Fileversion = %s ; Serverversion = %s", fversion, sversion)
	}
	validateFolder(absImportDir)
	runPackageFileSync(absImportDir)

	runImageFileSync(absImportDir, serverConfig)

	runImportSql(absImportDir, serverConfig)
	log.Info().Msg("import finished")
}

func getImportVersionProduct(path string) (string, string) {
	var versionfile string
	versionfile = path + "/version.txt"
	version, err := utils.ScannerFunc(versionfile, "version")
	if err != nil {
		log.Error().Msg("Version not found.")
	}
	product, err := utils.ScannerFunc(versionfile, "product_name")
	if err != nil {
		log.Fatal().Msg("Product not found")
	}
	log.Debug().Msgf("Import Product: %s; Version: %s", product, version)
	return version, product
}

func validateFolder(absImportDir string) {
	_, err := os.Stat(fmt.Sprintf("%s/sql_statements.sql.gz", absImportDir))
	if err != nil {
		if os.IsNotExist(err) {
			_, err = os.Stat(fmt.Sprintf("%s/sql_statements.sql", absImportDir))
			if err != nil {
				log.Fatal().Err(err).Msg("No usable .sql or .gz file found in import directory")
			}
		} else {
			log.Fatal().Err(err)
		}
	}
}

func hasConfigChannels(absImportDir string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/exportedConfigs.txt", absImportDir))
	log.Info().Err(err).Msg(fmt.Sprintf("no export config file found: %s/exportedConfigs.txt", absImportDir))
	return err == nil || os.IsExist(err)
}

func runPackageFileSync(absImportDir string) {
	packagesImportDir := fmt.Sprintf("%s/packages/", absImportDir)
	err := utils.FolderExists(packagesImportDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Info().Msg("no package files to import")
			return
		} else {
			log.Fatal().Err(err).Msg("Error getting import packages folder")
		}
	}

	rsyncParams := make([]string, 0)
	if log.Debug().Enabled() {
		rsyncParams = append(rsyncParams, "-v")
	}

	rsyncParams = append(rsyncParams, "-og", "--chown=wwwrun:www", "-r",
		packagesImportDir, "/var/spacewalk/packages/")

	cmd := exec.Command("rsync", rsyncParams...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("starting importing package files")
	err = cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("error importing package files")
	}
}

func runConfigFilesSync(labels []string, user string, password string) (interface{}, error) {
	client := xmlrpc.NewClient(user, password)
	return client.SyncConfigFiles(labels)
}

func runImageFileSync(absImportDir string, serverConfig string) {
	imagesImportDir := path.Join(absImportDir, "images")
	err := utils.FolderExists(imagesImportDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Info().Msg("No image files to import")
			return
		} else {
			log.Fatal().Err(err).Msg("Error reading import folder for images")
		}
	}

	rsyncParams := make([]string, 0)
	if log.Debug().Enabled() {
		rsyncParams = append(rsyncParams, "-v")
	}
	rsyncParams = append(rsyncParams, "-og", "--chown=salt:susemanager", "--chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r",
		"-r", "--exclude=pillars", imagesImportDir+"/", "/srv/www/os-images")

	cmd := exec.Command("rsync", rsyncParams...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("Copying image files")
	err = cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("Error importing image files")
	}

	pillarImportDir := path.Join(absImportDir, "images", "pillars")
	err = utils.FolderExists(pillarImportDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debug().Msg("No pillar files to import")
			return
		} else {
			log.Fatal().Err(err).Msg("Error reading import folder for pillars")
		}
	}

	log.Info().Msg("Copying image pillar files")
	pillarDumper.ImportImagePillars(pillarImportDir, utils.GetCurrentServerFQDN(serverConfig))
}

func importSqlFile(absImportDir string) {
	cmd := exec.Command("spacewalk-sql", fmt.Sprintf("%s/sql_statements.sql", absImportDir))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("Starting SQL import")
	err := cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msgf("Error running the SQL script")
	}
}

func importGzFile(absImportDir string) {
	cUnzip := exec.Command("gunzip", "-c", fmt.Sprintf("%s/sql_statements.sql.gz", absImportDir))
	cImport := exec.Command("spacewalk-sql", "-")

	pr, pw := io.Pipe()
	cUnzip.Stdout = pw
	cUnzip.Stderr = os.Stderr

	cImport.Stdin = pr
	cImport.Stdout = os.Stdout
	cImport.Stderr = os.Stderr

	log.Info().Msg("Starting SQL/GZ import")
	cUnzip.Start()
	cImport.Start()

	go func() {
		defer pw.Close()
		cUnzip.Wait()
	}()
	err := cImport.Wait()
	if err != nil {
		log.Fatal().Err(err).Msgf("Error running the SQL script")
	}
}

func runImportSql(absImportDir string, serverConfig string) {

	if _, err := os.Stat(fmt.Sprintf("%s/sql_statements.sql.gz", absImportDir)); err == nil {
		importGzFile(absImportDir)
	} else {
		if _, err := os.Stat(fmt.Sprintf("%s/sql_statements.sql", absImportDir)); err == nil {
			importSqlFile(absImportDir)
		}
	}

	pillarDumper.UpdateImagePillars(utils.GetCurrentServerFQDN(serverConfig))

	if hasConfigChannels(absImportDir) {
		labels := utils.ReadFileByLine(fmt.Sprintf("%s/exportedConfigs.txt", absImportDir))
		log.Debug().Msg("Will call xml-rpc API to update filesystem")
		_, err := runConfigFilesSync(labels, xmlRpcUser, xmlRpcPassword)
		if err != nil {
			log.Error().Err(err).Msgf(
				"Error recreating configuration files. Please run spacecmd api configchannel.syncSaltFilesOnDisk -A '[[%s]]'",
				strings.Join(labels, ", "),
			)
		}
	} else {
		log.Debug().Msg("No configuration channels, NO CALL to xml-rpc API")
	}
}
