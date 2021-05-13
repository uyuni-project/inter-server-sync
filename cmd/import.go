package cmd

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data to server",
	Run: runImport,
}

var importDir string

func init() {

	importCmd.Flags().StringVar(&importDir, "importDir", ".", "Location import data from")

	rootCmd.AddCommand(importCmd)
}

func runImport(cmd *cobra.Command, args []string) {
	log.Info().Msg(fmt.Sprintf("starting import from dir %s", importDir))
	validateFolder()
	runPackageFileSync()
	runImportSql()
	log.Info().Msg("import finished")
}

func validateFolder() {
	// FIXME need to be validate
	// maybe just check the sql file
}

func runPackageFileSync() {
	//rsync -og --chown=wwwrun:www -r packages/ /var/spacewalk/packages/
	cmd := exec.Command("rsync", "-og", "--chown=wwwrun:www", "-r",
		fmt.Sprintf("%s/packages/", importDir),
		"/var/spacewalk/packages/")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("starting importing package files")
	err := cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("error importing package files")
	}
}

func runImportSql() {
	cmd := exec.Command("spacewalk-sql", fmt.Sprintf("%s/sql_statements.sql", importDir))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Info().Msg("starting sql import")
	err := cmd.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("error running the sql script")
	}
}