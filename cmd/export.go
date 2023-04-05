package cmd

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
	"os"
	"path"
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
	cobra.OnInitialize(initConfig)

	exportCmd.Flags().StringVar(&config, "config", "", "Location of the configuration file")
	exportCmd.Flags().StringSlice("channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSlice("channelWithChildren", nil, "Channels to be exported")
	exportCmd.Flags().String("outputDir", ".", "Location for generated data")
	exportCmd.Flags().Bool("metadataOnly", false, "export only metadata")
	exportCmd.Flags().String("packagesOnlyAfter", "", "Only export packages added or modified after the specified date (date format can be 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss')")
	exportCmd.Flags().StringSlice("configChannels", nil, "Configuration Channels to be exported")
	exportCmd.Flags().Bool("images", false, "Export OS images and associated metadata")
	exportCmd.Flags().Bool("containers", false, "Export containers metadata")
	exportCmd.Flags().UintSlice("orgLimit", nil, "Export only for specified organizations")

	err := viper.BindPFlags(exportCmd.Flags())
	if err != nil {
		log.Warn().Err(err).Msg("Failed to bind PFlags")
	}
	exportCmd.Args = cobra.NoArgs

	rootCmd.AddCommand(exportCmd)
}

func initConfig() {
	if config != "" {
		viper.SetConfigFile(utils.GetAbsPath(config))

		if err := viper.ReadInConfig(); err != nil {
			log.Panic().Err(err).Msg("Failed to read config file")
		}
	}
}

func runExport(cmd *cobra.Command, args []string) {
	log.Info().Msg("Export started")
	// check output dir existence and create it if needed.

	channels = viper.GetStringSlice("channels")
	channelWithChildren = viper.GetStringSlice("channelWithChildren")
	outputDir = viper.GetString("outputDir")
	metadataOnly = viper.GetBool("metadataOnly")
	startingDate = viper.GetString("packagesOnlyAfter")
	configChannels = viper.GetStringSlice("configChannels")
	includeImages = viper.GetBool("images")
	includeContainers = viper.GetBool("containers")
	// https://github.com/spf13/viper/issues/926
	rawOrgs := viper.GetString("orgLimit")
	var parsedOrgs []uint
	if err := json.Unmarshal([]byte(rawOrgs), &parsedOrgs); err != nil {
		log.Panic().Err(err).Msg("Failed to parse orgLimit")
	}
	orgs = parsedOrgs

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

	log.Info().Msgf("Export done. Directory: %s", outputDir)
}
