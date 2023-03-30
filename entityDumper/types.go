package entityDumper

import (
	"github.com/uyuni-project/inter-server-sync/utils"
)

type ExportConfig struct {
	ServerConfig        string
	Channels            []string
	ChannelWithChildren []string
	OutputDir           string
	MetadataOnly        bool
	StartingDate        string
	ConfigChannels      []string
	IncludeImages       bool
	IncludeContainers   bool
	Orgs                []uint
}

type DumperOptions struct {
	ServerConfig              string
	ChannelLabels             []string
	ConfigLabels              []string
	ChannelWithChildrenLabels []string
	OutputFolder              string
	outputFolderAbsPath       string
	MetadataOnly              bool
	StartingDate              string
	Containers                bool
	OSImages                  bool
	Orgs                      []uint
}

func (opt *DumperOptions) GetOutputFolderAbsPath() string {
	if "" == opt.outputFolderAbsPath {
		opt.outputFolderAbsPath = utils.GetAbsPath(opt.OutputFolder)
	}
	return opt.outputFolderAbsPath
}

type channelsProcess struct {
	channelsMap map[string]bool
	channels    []string
}

func (c *channelsProcess) addChannelLabel(label string) {
	c.channelsMap[label] = true
	c.channels = append(c.channels, label)
}
