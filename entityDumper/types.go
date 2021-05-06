package entityDumper

import (
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"strings"
)

type ChannelDumperOptions struct {
	ServerConfig string
	ChannelLabels []string
	ChannelWithChildrenLabels []string
	OutputFolder string
	outputFolderAbsPath string
	MetadataOnly bool
}

func (opt *ChannelDumperOptions) getOutputFolderAbsPath() string {
	if "" == opt.outputFolderAbsPath {
		outputFolder := opt.OutputFolder
		if filepath.IsAbs(outputFolder) {
			outputFolder, _ = filepath.Abs(outputFolder)
		} else {
			homedir, err := os.UserHomeDir()
			if err != nil {
				log.Fatal().Msg("Couldn't determine the home directory")
				panic(err)
			}
			if strings.HasPrefix(outputFolder, "~") {
				outputFolder = strings.Replace(outputFolder, "~", homedir, -1)
			}
		}
		opt.outputFolderAbsPath =outputFolder
	}
	return opt.outputFolderAbsPath
}

type channelsProcess struct {
	channelsMap map[string] bool
	channels    []string
}

func (c *channelsProcess) addChannelLabel(label string)  {
	c.channelsMap[label] = true
	c.channels = append(c.channels, label)
}
