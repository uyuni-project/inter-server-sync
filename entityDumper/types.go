package entityDumper

type ChannelDumperOptions struct {
	ServerConfig string
	ChannelLabels []string
	ChannelWithChildrenLabels []string
	OutputFolder string
	MetadataOnly bool
}

type channelsProcess struct {
	channelsMap map[string] bool
	channels    []string
}

func (c *channelsProcess) addChannelLabel(label string)  {
	c.channelsMap[label] = true
	c.channels = append(c.channels, label)
}
