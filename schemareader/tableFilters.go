package schemareader

const (
	VirtualIndexName = "virtual_main_unique_index"
)

func applyTableFilters(table Table) Table {
	switch table.Name {
	case "rhnchecksumtype":
		table.PKSequence = "rhn_checksum_id_seq"
	case "rhnChecksum":
		table.PKSequence = "rhnChecksum_seq"
	case "rhnpackagearch":
		table.PKSequence = "rhn_package_arch_id_seq"
	case "rhnchannelarch":
		table.PKSequence = "rhn_channel_arch_id_seq"
	case "rhnpackagename":
		// constraint: rhn_pn_id_pk
		table.PKSequence = "RHN_PKG_NAME_SEQ"
	case "rhnpackagenevra":
		table.PKSequence = "rhn_pkgnevra_id_seq"
	case "rhnpackagesource":
		table.PKSequence = "rhn_package_source_id_seq"
	case "rhnpackageevr":
		// constraint: rhn_pe_id_pk
		table.PKSequence = "rhn_pkg_evr_seq"
		unexportColumns := make(map[string]bool)
		unexportColumns["type"] = true
		table.UnexportColumns = unexportColumns
		table.UniqueIndexes["rhn_pe_v_r_e_uq"] = UniqueIndex{Name: "rhn_pe_v_r_e_uq",
			Columns: append(table.UniqueIndexes["rhn_pe_v_r_e_uq"].Columns, "type")}
		table.UniqueIndexes["rhn_pe_v_r_uq"] = UniqueIndex{Name: "rhn_pe_v_r_uq",
			Columns: append(table.UniqueIndexes["rhn_pe_v_r_uq"].Columns, "type")}
	case "rhnpackage":
		// We need to add a virtual unique constraint
		table.PKSequence = "RHN_PACKAGE_ID_SEQ"
		virtualIndexColumns := []string{"name_id", "evr_id", "package_arch_id", "checksum_id", "org_id"}
		table.UniqueIndexes[VirtualIndexName] = UniqueIndex{Name: VirtualIndexName, Columns: virtualIndexColumns}
		table.MainUniqueIndexName = VirtualIndexName
	case "rhnpackagechangelogdata":
		// We need to add a virtual unique constraint
		table.PKSequence = "rhn_pkg_cld_id_seq"
		virtualIndexColumns := []string{"name", "text", "time"}
		table.UniqueIndexes[VirtualIndexName] = UniqueIndex{Name: VirtualIndexName, Columns: virtualIndexColumns}
		table.MainUniqueIndexName = VirtualIndexName
	case "rhnpackagechangelogrec":
		table.PKSequence = "rhn_pkg_cl_id_seq"

	case "rhnpackagecapability":
		// pkid: rhn_pkg_capability_id_pk
		table.PKSequence = "RHN_PKG_CAPABILITY_ID_SEQ"
		// table has real unique index, but they are complex and useless, since we do nothing in the conflict
		// to simplify the code we can create a virtual index that will insure all data exists as supposed
		virtualIndexColumns := []string{"name", "version"}
		table.UniqueIndexes[VirtualIndexName] = UniqueIndex{Name: VirtualIndexName, Columns: virtualIndexColumns}
		table.MainUniqueIndexName = VirtualIndexName
	}
	return table
}
