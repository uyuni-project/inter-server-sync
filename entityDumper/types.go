package entityDumper

import (
	"github.com/uyuni-project/inter-server-sync/utils"
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
		opt.outputFolderAbsPath = utils.GetAbsPath(opt.OutputFolder)
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
