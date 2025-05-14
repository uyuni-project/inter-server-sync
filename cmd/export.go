// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"errors"
	"os"
	"path"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export server entities to be imported in other server",
	Run:   runExport,
}

var channels []string
var channelWithChildren []string
var configChannels []string
var outputDir string
var metadataOnly bool
var startingDate string
var includeImages bool
var includeContainers bool
var signKey string
var pubcert string
var orgs []uint

func init() {
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSliceVar(&channelWithChildren, "channel-with-children", nil, "Channels to be exported")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")
	exportCmd.Flags().StringVar(&startingDate, "packagesOnlyAfter", "", "Only export packages added or modified after the specified date (date format can be 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss')")
	exportCmd.Flags().StringSliceVar(&configChannels, "configChannels", nil, "Configuration Channels to be exported")
	exportCmd.Flags().BoolVar(&includeImages, "images", false, "Export OS images and associated metadata")
	exportCmd.Flags().BoolVar(&includeContainers, "containers", false, "Export containers metadata")
	exportCmd.Flags().UintSliceVar(&orgs, "orgLimit", nil, "Export only for specified organizations")
	exportCmd.Flags().StringVar(&signKey, "signKey", "/etc/pki/tls/private/spacewalk.key", "Private certificate used for signing the export")
	exportCmd.Flags().StringVar(&pubcert, "certificate", "/etc/pki/tls/certs/spacewalk.crt", "Public certificate to be included in the export. Subject of CA validation during import")
	exportCmd.Args = cobra.NoArgs

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) {
	log.Info().Msg("Export started")
	// check output dir existence and create it if needed.

	// Validate data
	validatedDate, ok := utils.ValidateDate(startingDate)
	if !ok {
		log.Fatal().Msg("Unable to validate the date. Allowed formats are 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'")
	}

	// Validate we have signing key
	if _, err := os.Stat(signKey); errors.Is(err, os.ErrNotExist) {
		log.Fatal().Msg("Signing key does not exists. Please use `--signKey` to set key for export signing.")
	}

	options := entityDumper.DumperOptions{
		ServerConfig:              serverConfig,
		ChannelLabels:             channels,
		ConfigLabels:              configChannels,
		ChannelWithChildrenLabels: channelWithChildren,
		OutputFolder:              outputDir,
		MetadataOnly:              metadataOnly,
		StartingDate:              validatedDate,
		OSImages:                  includeImages,
		Containers:                includeContainers,
		Orgs:                      orgs,
		SignKey:                   signKey,
	}
	entityDumper.DumpAllEntities(options)

	var versionfile string
	versionfile = path.Join(utils.GetAbsPath(outputDir), "version.txt")
	vf, err := os.Open(versionfile)
	defer vf.Close()
	if os.IsNotExist(err) {
		f, err := os.Create(versionfile)
		if err != nil {
			log.Panic().Msg("Unable to create version file")
		}
		vf = f
	}
	version, product := utils.GetCurrentServerVersion(serverConfig)
	vf.WriteString("product_name = " + product + "\n" + "version = " + version + "\n")

	// Collect public key of used signing key to the export. Will be CA validated during import
	if _, err := dumper.Copy(pubcert, path.Join(utils.GetAbsPath(outputDir), "hubserver.pem")); err != nil {
		log.Error().Err(err).Msg("failed to collect hub server public certificate. Manual selection will be needed on the import.")
	}
	log.Info().Msgf("Export done. Directory: %s", outputDir)
}
