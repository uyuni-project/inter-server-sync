package schemareader

func applyTableFilters(table Table) Table {
	if len(table.PKSequence) == 0 {
		switch table.Name {
		case "rhnchecksumtype":
			table.PKSequence = "rhn_checksum_id_seq"
		case "rhnpackagearch":
			table.PKSequence = "rhn_package_arch_id_seq"
		case "rhnchannelarch":
			table.PKSequence = "rhn_channel_arch_id_seq"
		case "rhnpackagename":
			// constraint: rhn_pn_id_pk
			table.PKSequence = "rhn_checksum_id_seq"
		case "rhnpackageevr":
			// constraint: rhn_pe_id_pk
			table.PKSequence = "rhn_pkg_evr_seq"
		}
	}
	return table
}
