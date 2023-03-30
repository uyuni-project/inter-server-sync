package entityDumper

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/utils"
	"io"
	"os"
)

func SetOptionsByConfig(configPath string, options *DumperOptions) {
	var config ExportConfig

	if configPath == "" {
		return
	}

	configFile, err := os.Open(utils.GetAbsPath(configPath))
	if err != nil {
		log.Panic().Err(err).Msg("failed to open config file")
	}
	defer func(configFile *os.File) {
		err := configFile.Close()
		if err != nil {
			log.Warn().Err(err).Msg("failed to close config file")
		}
	}(configFile)

	configBytes, err := io.ReadAll(configFile)
	if err != nil {
		log.Panic().Err(err).Msg("failed to read config file")
	}

	if err = json.Unmarshal(configBytes, &config); err != nil {
		log.Panic().Err(err).Msg("failed to unmarshal config file")
	}

	if options.ServerConfig == "/etc/rhn/rhn.conf" && config.ServerConfig != "" {
		options.ServerConfig = config.ServerConfig
	}

	if options.OutputFolder == "." && config.OutputDir != "" {
		options.OutputFolder = config.OutputDir
	}

	if len(options.ChannelLabels) == 0 {
		options.ChannelLabels = config.Channels
	}

	if len(options.ChannelWithChildrenLabels) == 0 {
		options.ChannelWithChildrenLabels = config.ChannelWithChildren
	}

	if len(options.ConfigLabels) == 0 {
		options.ConfigLabels = config.ConfigChannels
	}

	if config.MetadataOnly {
		options.MetadataOnly = config.MetadataOnly
	}

	if options.StartingDate == "" {
		options.StartingDate = config.StartingDate
	}

	if len(options.Orgs) == 0 {
		options.Orgs = config.Orgs
	}

	if config.IncludeImages {
		options.OSImages = config.IncludeImages
	}

	if config.IncludeContainers {
		options.Containers = config.IncludeContainers
	}
}
