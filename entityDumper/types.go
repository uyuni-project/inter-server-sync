package entityDumper

import (
	"github.com/uyuni-project/inter-server-sync/utils"
)

type DumperOptions struct {
	ServerConfig              string
	ChannelLabels             []string
	ConfigLabels              []string
	ChannelWithChildrenLabels []string
	OutputFolder              string
	outputFolderAbsPath       string
	MetadataOnly              bool
	StartingDate              string
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
