package cmd

import (
	"os"
	"path"
	"strings"

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

var channels []string
var channelWithChildren []string
var configChannels []string
var outputDir string
var metadataOnly bool
var startingDate string
var includeImages bool
var includeContainers bool
var orgidOnly uint

func init() {
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSliceVar(&channelWithChildren, "channel-with-children", nil, "Channels to be exported")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")
	exportCmd.Flags().StringVar(&startingDate, "packagesOnlyAfter", "", "Only export packages added or modified after the specified date (date format can be 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss')")
	exportCmd.Flags().StringSliceVar(&configChannels, "configChannels", nil, "Configuration Channels to be exported")
	exportCmd.Flags().BoolVar(&includeImages, "images", false, "Export OS images and associated metadata")
	exportCmd.Flags().BoolVar(&includeContainers, "containers", false, "Export containers metadata")
	exportCmd.Flags().UintVar(&orgidOnly, "orgId", 0, "Export only for organization id")
	exportCmd.Args = cobra.NoArgs

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) {
	log.Debug().Msg("export called")
	log.Debug().Msg(strings.Join(channels, ","))
	log.Debug().Msg(outputDir)
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

	if len(channels) > 0 || len(channelWithChildren) > 0 {
		options := entityDumper.ChannelDumperOptions{
			ServerConfig:              serverConfig,
			ChannelLabels:             channels,
			ChannelWithChildrenLabels: channelWithChildren,
			OutputFolder:              outputDir,
			MetadataOnly:              metadataOnly,
			StartingDate:              validatedDate,
		}
		entityDumper.DumpChannelData(options)
	}

	if includeImages || includeContainers {
		imageOptions := entityDumper.ImageDumperOptions{
			ServerConfig: serverConfig,
			OutputFolder: outputDir,
			OSImage:      includeImages,
			Containers:   includeContainers,
			OrgID:        orgidOnly,
			StartingDate: validatedDate,
		}
		entityDumper.DumpImageData(imageOptions)
	}

	log.Info().Msgf("Export done. Directory: %s", outputDir)
}
