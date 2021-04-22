package cmd

import (
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
)

// dotCmd represents the dot command
var dotCmd = &cobra.Command{
	Use:   "dot",
	Short: "export database schema as dot diagram",
	Run: func(cmd *cobra.Command, args []string) {
		db := schemareader.GetDBconnection(serverConfig)
		defer db.Close()
		tables := schemareader.ReadTablesSchema(db, entityDumper.SoftwareChannelTableNames())
		schemareader.DumpToGraphviz(tables)
	},
}

func init() {
	rootCmd.AddCommand(dotCmd)
}
