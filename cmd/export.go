package cmd

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"strings"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export server entities to be imported in other server",
	Run: runExport,
}

var channels []string
var outputDir string
var metadataOnly bool
func init() {
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.MarkFlagRequired("channels")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) {
	log.Debug().Msg("export called")
	log.Debug().Msg(strings.Join(channels, ","))
	log.Debug().Msg(outputDir)

	db := schemareader.GetDBconnection(serverConfig)
	defer db.Close()
	tableData := entityDumper.DumpChannelData(db, channels, outputDir, metadataOnly)
	// FIXME the nextcode should be removed and log done inside dumper.
	for index, channelTableData := range tableData {
		log.Debug().Msg(fmt.Sprintf("Processing channe %d...", index))
		for path := range channelTableData.Paths {
			log.Debug().Msg(path)
		}
		count := 0
		for _, value := range channelTableData.TableData {
			log.Debug().Msg(fmt.Sprintf("%s number inserts: %d \n\t %s keys: %s\n", value.TableName, len(value.Keys),
				value.TableName, value.Keys))
			count = count + len(value.Keys)
		}
		log.Debug().Msg(fmt.Sprintf("IDS############%d\n\n", count))
	}
}