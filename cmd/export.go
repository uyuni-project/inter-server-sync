package cmd

import (
	"os"
	"path"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export server entities to be imported in other server",
	Run:   runExport,
}

var config string
var channels []string
var channelWithChildren []string
var configChannels []string
var outputDir string
var metadataOnly bool
var startingDate string
var includeImages bool
var includeContainers bool
var orgs []uint

func init() {
	exportCmd.Flags().StringVar(&config, "config", "", "Location of configuration file")
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSliceVar(&channelWithChildren, "channel-with-children", nil, "Channels to be exported")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")
	exportCmd.Flags().StringVar(&startingDate, "packagesOnlyAfter", "", "Only export packages added or modified after the specified date (date format can be 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss')")
	exportCmd.Flags().StringSliceVar(&configChannels, "configChannels", nil, "Configuration Channels to be exported")
	exportCmd.Flags().BoolVar(&includeImages, "images", false, "Export OS images and associated metadata")
	exportCmd.Flags().BoolVar(&includeContainers, "containers", false, "Export containers metadata")
	exportCmd.Flags().UintSliceVar(&orgs, "orgLimit", nil, "Export only for specified organizations")
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
	}
	entityDumper.SetOptionsByConfig(config, &options)
	entityDumper.DumpAllEntities(options)
	var versionfile string
	versionfile = path.Join(utils.GetAbsPath(options.OutputFolder), "version.txt")
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

	log.Info().Msgf("Export done. Directory: %s", options.OutputFolder)
}
