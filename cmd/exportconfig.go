package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
	"os"
	"strings"
)

var exportconfigsCmd = &cobra.Command{
	Use:   "exportconfig",
	Short: "Export server configurations to be imported in other server",
	Run:   runExportConfigs,
}

func init() {
	exportconfigsCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportconfigsCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")
	exportconfigsCmd.Flags().StringSliceVar(&labels, "labels", nil, "Configuration Channels to be exported")
	rootCmd.AddCommand(exportconfigsCmd)
}

func runExportConfigs(cmd *cobra.Command, args []string) {
	log.Debug().Msg("export of configs called")
	log.Debug().Msg(strings.Join(channels, ","))
	log.Debug().Msg(outputDir)

	options := entityDumper.ChannelDumperOptions{
		ServerConfig:              serverConfig,
		OutputFolder:              outputDir,
		MetadataOnly:              metadataOnly,
		ConfigLabels:              labels,
	}
	utils.ValidateExportFolder(options.GetOutputFolderAbsPath())
	entityDumper.DumpConfigs(options)
	var versionfile string
	versionfile = options.GetOutputFolderAbsPath() + "/version.txt"
	vf, err := os.Open(versionfile)
	defer vf.Close()
	if os.IsNotExist(err) {
		f, err := os.Create(versionfile)
		if err != nil {
			log.Fatal().Msg("Unable to create version file")
		}
		vf = f
	}
	version, product := utils.GetCurrentServerVersion()
	vf.WriteString("product_name = " + product + "\n" + "version = " + version + "\n")
}
