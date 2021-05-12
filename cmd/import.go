package cmd

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"os"
	"os/exec"
)

// importCmd represents the import command
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
	//Mgr-sync needs to be disabled
	//Run sql script
	runImportSql()
	//Detect it's a channel import with packages and copy package files to final location
	runPackageFileSync()
	//Errata cache and repo metadata are regenerated after import (Taskomatic)

	log.Info().Msg("import finished")
}

func validateFolder() {
	// FIXME
	// validate import folder exists
	// validate all mandatory files exists
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

func updateNeededCache(channelID int) {
	db := schemareader.GetDBconnection(serverConfig)
	cacheQuery := "select rhn_channel.update_needed_cache((select id from rhnchannel where label ='sle-product-suse-manager-server-4.1-pool-x86_64'));"
	serverIDs := fmt.Sprintf(`SELECT sc.server_id as id FROM rhnServerChannel sc WHERE sc.channel_id = %s order by id asc;`, channelID)
	rows, err := db.Query(serverIDs)
	if err != nil {
		log.Fatal().Err(err).Msg("error executing cache query")
		panic(err)
	}
	for rows.Next() {
		server, err := db.Query(sql)
		if err != nil {
			log.Fatal().Err(err).Msg("error executing server query")
		}

	}

}