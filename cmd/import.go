package cmd

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/utils"
	"github.com/uyuni-project/inter-server-sync/xmlrpc"
	"os"
	"os/exec"
	"strings"
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
	sversion, sproduct := utils.GetCurrentServerVersion()
	if fversion != sversion || fproduct != sproduct {
		log.Panic().Msgf("Wrong version detected. Fileversion = %s ; Serverversion = %s", fversion, sversion)
	}
	validateFolder(absImportDir)
	runPackageFileSync(absImportDir)
	runImportSql(absImportDir)
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
	_, err := os.Stat(fmt.Sprintf("%s/sql_statements.sql", absImportDir))
	if os.IsNotExist(err) {
		log.Fatal().Err(err).Msg("No usable .sql files found in import directory")
	}
}

func hasConfigChannels(absImportDir string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/exportedConfigs.txt", absImportDir))
	return os.IsExist(err)
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

	cmd := exec.Command("rsync", "-og", "--chown=wwwrun:www", "-r",
		packagesImportDir, "/var/spacewalk/packages/")
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

func runImportSql(absImportDir string) {

	cmd := exec.Command("spacewalk-sql", fmt.Sprintf("%s/sql_statements.sql", absImportDir))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("starting sql import")
	err := cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("error running the sql script")
	}

	if hasConfigChannels(absImportDir) {
		labels := utils.ReadFileByLine(fmt.Sprintf("%s/exportedConfigs.txt", absImportDir))
		_, err = runConfigFilesSync(labels, xmlRpcUser, xmlRpcPassword)
		if err != nil {
			log.Error().Err(err).Msgf(
				"Error recreating configuration files. Please run spacecmd api configchannel.syncSaltFilesOnDisk -A '[[%s]]'",
				strings.Join(labels, ", "),
			)
		}
	}
}
