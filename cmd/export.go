package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"strings"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export server entities to be imported in other server",
	Run: runExport,
}

var channels []string
var channelWithChildren []string
var outputDir string
var metadataOnly bool
func init() {
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSliceVar(&channelWithChildren, "channel-with-children", nil, "Channels to be exported")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) {
	log.Debug().Msg("export called")
	log.Debug().Msg(strings.Join(channels, ","))
	log.Debug().Msg(outputDir)
	// check output dir existance and create it if needed.

	options := entityDumper.ChannelDumperOptions{
		ServerConfig: serverConfig,
		ChannelLabels: channels,
		ChannelWithChildrenLabels: channelWithChildren,
		OutputFolder: outputDir,
		MetadataOnly: metadataOnly,
	}

	entityDumper.DumpChannelData(options)
}
