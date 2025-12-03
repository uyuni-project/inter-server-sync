// SPDX-FileCopyrightText: 2025 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package cobbler

import (
	"database/sql"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

const (
	COBBLER         = "/usr/bin/cobbler"
	OS_STORE_PREFIX = "/srv/www/os-images"
	DEFAULT_IMAGE   = "DEFAULT_IMAGE"
)

// For unit testing
var cmd = exec.Command

type Image struct {
	Id       string
	Name     string
	Version  string
	Revision string
	Org      string
	OrgId    string
	Kernel   string
	Initrd   string
}

func (i Image) String() string {
	return fmt.Sprintf("Image id %s, name %s, version %s, revision %s under orgid %s, org %s with kernel %s and initrd %s",
		i.Id, i.Name, i.Version, i.Revision, i.OrgId, i.Org, i.Kernel, i.Initrd)
}

type Group struct {
	BranchId       string
	Image          string
	ImageVersion   string
	KernelLine     string
	Org            string
	OrgId          string
	NoPrefix       bool
	NoSuffix       bool
	Server         string
	TerminalNaming string
}

func (g Group) String() string {
	return fmt.Sprintf("Group id %s, server %s, default image %s, version %s under orgid %s, org %s with kernel line %s",
		g.BranchId, g.Server, g.Image, g.ImageVersion, g.OrgId, g.Org, g.KernelLine)
}

func RecreateCobblerEntities(serverconfig string) error {
	db := schemareader.GetDBconnection(serverconfig)
	if err := processImages(db); err != nil {
		return err
	}
	log.Info().Msg("Recomputing saltboot groups")
	return processGroups(db)
}

//// Groups

func processGroups(db *sql.DB) error {
	// Query DB for all saltboot groups, including pillar data
	groups := sqlUtil.ExecuteQueryWithResults(db,
		`SELECT rsg.name,
		pillar->'saltboot'->>'download_server' AS server,
		(pillar->'saltboot'->'disable_id_prefix')::bool AS disableprefix,
		(pillar->'saltboot'->'disable_unique_suffix')::bool AS disablesuffix,
		pillar->'saltboot'->>'default_boot_image' AS image,
		pillar->'saltboot'->>'default_boot_image_version' AS imageversion,
		pillar->'saltboot'->>'default_kernel_parameters' AS kernelline,
		pillar->'saltboot'->>'minion_id_naming' AS naming,
		wc.id::text AS orgid, wc.name AS orgname FROM
		rhnservergroup rsg LEFT JOIN susesaltpillar sp ON rsg.id = sp.group_id INNER JOIN
		web_customer wc on rsg.org_id = wc.id WHERE sp.category = 'formula-saltboot-group';`)
	for _, dbgroup := range groups {
		group := Group{}
		for _, column := range dbgroup {
			switch column.ColumnName {
			case "name":
				group.BranchId = column.Value.(string)
			case "orgid":
				group.OrgId = column.Value.(string)
			case "orgname":
				group.Org = column.Value.(string)
			case "kernelline":
				group.KernelLine = column.Value.(string)
			case "image":
				group.Image = column.Value.(string)
			case "imageversion":
				group.ImageVersion = column.Value.(string)
			case "disablesuffix":
				group.NoPrefix = column.Value.(bool)
			case "disableprefix":
				group.NoSuffix = column.Value.(bool)
			case "server":
				group.Server = column.Value.(string)
			case "naming":
				group.TerminalNaming = column.Value.(string)
			default:
				log.Debug().Msgf("Unexpected column %s", column.ColumnName)
			}
		}
		// Construct default kernel line:
		kernel := "MINION_ID_PREFIX=" + group.BranchId + " MASTER=" + group.Server
		if group.NoPrefix {
			kernel = kernel + " DISABLE_ID_PREFIX=1"
		}
		if group.NoSuffix {
			kernel = kernel + " DISABLE_UNIQUE_SUFFIX=1"
		}
		switch group.TerminalNaming {
		case "FQDN":
			kernel = kernel + " USE_FQDN_MINION_ID=1"
		case "HWType":
			kernel = kernel + " DISABLE_HOSTNAME_ID=1"
		case "MAC":
			kernel = kernel + " USE_MAC_MINION_ID=1"
		default:
			//ignore
		}
		group.KernelLine = kernel + group.KernelLine
		log.Debug().Msgf("Recognized group: %s", group.String())
		if err := createGroupEntry(group); err != nil {
			log.Error().Err(err).Msgf("Failed to create SaltbootGroup entry for group %s", group.BranchId)
		}
	}
	return nil
}

func createGroupEntry(group Group) error {
	name := makeCobblerName(group.BranchId, group.Org, group.OrgId)
	log.Debug().Msgf("Creating cobbler profile %s", name)

	// Detect image
	var image string
	if len(group.Image) > 0 {
		image = makeCobblerName(group.Image, group.Org, group.OrgId)
		if len(group.ImageVersion) > 0 {
			version := strings.SplitN(group.ImageVersion, "-", 2)
			if len(version) == 2 {
				image, _ = makeCobblerNameVR(group.Image, version[0], version[1], group.Org, group.OrgId)
			} else {
				_, image = makeCobblerNameVR(group.Image, version[0], "0", group.Org, group.OrgId)
			}
		}
	} else {
		image = makeCobblerName(DEFAULT_IMAGE, group.Org, group.OrgId)
	}
	log.Debug().Msgf("Profile %s detected for the group %s", image, group.BranchId)

	exists, err := cobblerItemExists(name, "profile")
	if err != nil {
		return err
	}
	action := "add"
	if exists {
		action = "edit"
	}
	if err := cmd(COBBLER, "profile", action, "--parent", image, "--name", name, "--enable-menu", "yes",
		"--kernel-options", group.KernelLine).Run(); err != nil {
		log.Error().Msgf("Error %sing existing profile %s with parent %s", action, name, image)
		return err
	}
	return nil
}

//// Images

func processImages(db *sql.DB) error {
	// Query DB for all os-images and create distros and profiles for the images
	images := []Image{}
	dbimages := sqlUtil.ExecuteQueryWithResults(db,
		`SELECT II.id::text, II.name, II.org_id::text, WC.name AS orgname, version, curr_revision_num::text FROM
		suseimageinfo AS II INNER JOIN web_customer AS WC ON II.org_id = WC.id
		WHERE image_type = 'kiwi' and built = 'Y'`)
	for _, dbimage := range dbimages {
		image := Image{}
		for _, column := range dbimage {
			switch column.ColumnName {
			case "id":
				image.Id = column.Value.(string)
			case "name":
				image.Name = column.Value.(string)
			case "version":
				image.Version = column.Value.(string)
			case "org_id":
				image.OrgId = column.Value.(string)
			case "orgname":
				image.Org = column.Value.(string)
			case "curr_revision_num":
				image.Revision = column.Value.(string)
			default:
				log.Error().Msgf("Unexpected column %s", column.ColumnName)
			}
		}
		// Get image files for the image
		files := sqlUtil.ExecuteQueryWithResults(db,
			"SELECT file, type FROM suseimagefile WHERE image_info_id = $1", image.Id)
		for _, file := range files {
			var tmpfile string
			var filetype string
			for _, column := range file {
				switch column.ColumnName {
				case "file":
					tmpfile = column.Value.(string)
				case "type":
					filetype = column.Value.(string)
				default:
					log.Debug().Msgf("Unexpected column %s", column.ColumnName)
				}
			}
			switch filetype {
			case "kernel":
				image.Kernel = path.Join(OS_STORE_PREFIX, image.OrgId, tmpfile)
			case "initrd":
				image.Initrd = path.Join(OS_STORE_PREFIX, image.OrgId, tmpfile)
			default:
			}
		}
		if len(image.Kernel) == 0 || len(image.Initrd) == 0 {
			log.Warn().Msgf("Unable to get kernel or initrd entries for image %s. Ignoring the image", image.Name)
			continue
		}
		log.Debug().Msg(image.String())
		if err := createDistroEntry(image); err != nil {
			log.Error().Err(err).Msgf("Error when creating cobbler entry for image %s", image.Name)
			continue
		}
		images = append(images, image)
	}
	return updateDistroProfiles(images)
}

func createDistroEntry(image Image) error {
	nameVR, _ := makeCobblerNameVR(image.Name, image.Version, image.Revision, image.Org, image.OrgId)
	if len(nameVR) == 0 {
		return fmt.Errorf("Unable to create all expected distro names for %s. This is a bug.", image.Name)
	}
	log.Debug().Msgf("Creating cobbler distro %s", nameVR)
	if exists, err := cobblerItemExists(nameVR, "distro"); err != nil {
		return err
	} else {
		if exists {
			log.Debug().Msgf("Distro %s is already present", nameVR)
			return nil
		}
	}
	command := cmd(COBBLER, "distro", "add", "--breed", "generic", "--kernel", image.Kernel,
		"--initrd", image.Initrd, "--name", nameVR,
		"--comment", "Distro for image "+image.Name+"-"+image.Version+"-"+image.Revision+" belonging to organization "+image.Org,
		"--kernel-options", "panic=60 splash=silent")
	if err := command.Run(); err != nil {
		return err
	}
	if err := cmd(COBBLER, "profile", "add", "--distro", nameVR, "--name", nameVR, "--enable-menu", "no").Run(); err != nil {
		return err
	}
	return nil
}

// For each distro name this refreshes distro profile nameV and distro profile N to point to the latest versions
func updateDistroProfiles(images []Image) error {
	imageMap := map[string]Image{}
	var latestImage Image
	// Get latest versions for each image name
	for _, image := range images {
		if storedImage, ok := imageMap[image.Name]; !ok {
			imageMap[image.Name] = image
			if isVersionNewer(image, latestImage) {
				latestImage = image
			}
		} else {
			if isVersionNewer(image, storedImage) {
				imageMap[image.Name] = image
				if isVersionNewer(image, latestImage) {
					latestImage = image
				}
			}
		}
	}
	log.Debug().Msgf("Latest image: %s", latestImage.String())

	// Now we have imageMap with distinct images where those images are the newest versions
	for _, image := range imageMap {
		nameVR, nameR := makeCobblerNameVR(image.Name, image.Version, image.Revision, image.Org, image.OrgId)
		name := makeCobblerName(image.Name, image.Org, image.OrgId)

		for _, n := range []string{nameR, name} {
			exists, err := cobblerItemExists(n, "profile")
			if err != nil {
				return err
			}
			action := "add"
			if exists {
				action = "edit"
			}
			if err := cmd(COBBLER, "profile", action, "--distro", nameVR, "--name", n, "--enable-menu", "no").Run(); err != nil {
				log.Error().Msgf("Error %sing existing profile %s", action, n)
				return err
			}
		}
	}

	// We don't have build info to determine latest image build, so we take the latest version as the default image
	defaultName := makeCobblerName(DEFAULT_IMAGE, latestImage.Org, latestImage.OrgId)
	latestName, _ := makeCobblerNameVR(latestImage.Name, latestImage.Version, latestImage.Revision, latestImage.Org,
		latestImage.OrgId)
	exists, err := cobblerItemExists(defaultName, "profile")
	if err != nil {
		return err
	}
	action := "add"
	if exists {
		action = "edit"
	}
	if err := cmd(COBBLER, "profile", action, "--distro", latestName, "--name", defaultName, "--enable-menu", "no").Run(); err != nil {
		log.Error().Msgf("Error %sing existing profile %s", action, defaultName)
		return err
	}
	return nil
}

//// Utils

// isVersionNewer returns true if image1 is newer than image2
func isVersionNewer(image1, image2 Image) bool {
	// Trivial case
	if len(image1.Version) == 0 {
		return false
	}

	v1 := parseToSegments(image1.Version, image1.Revision)
	v2 := parseToSegments(image2.Version, image2.Revision)

	for k := 0; k < len(v1) && k < len(v2); k++ {
		if v1[k] != v2[k] {
			// For descending order (Newest First), we want the larger number
			return v1[k] > v2[k]
		}
	}
	return false
}

// parseToSegments converts version and revision into 4 member array
func parseToSegments(ver string, rev string) []uint {
	// Version is M.m.b
	parts := strings.Split(ver, ".")
	segments := make([]uint, 0, len(parts)+1)

	for _, p := range parts {
		val, _ := strconv.ParseUint(p, 10, 0)
		segments = append(segments, uint(val))
	}
	val, _ := strconv.ParseUint(rev, 10, 0)
	return append(segments, uint(val))
}

func cobblerItemExists(name string, item string) (bool, error) {
	command := cmd(COBBLER, item, "find", "--name", name)
	if out, err := command.Output(); err != nil {
		log.Error().Msgf("Failed to query for existing %s %s", item, name)
		return false, err
	} else {
		return len(out) >= len(name), nil
	}
}

/*
makeCobblerName returns custom saltboot name:

	name:S:orgid:org
*/
func makeCobblerName(inname string, org string, orgid string) string {
	return strings.Join([]string{inname, "S", orgid, org}, ":")
}

/*
makeCobblerNameVR returns custom saltboot names:

	nameVR: name-version-revision:S:orgid:org
	nameV:  name-version:S:orgid:org
*/
func makeCobblerNameVR(name string, version string, revision string, org string, orgid string) (string, string) {
	if len(version) == 0 {
		log.Error().Msg("Wrong call to the naming function. This is a bug")
		return "", ""
	}
	nameVR := strings.Join([]string{name, version, revision}, "-")
	nameVR = strings.Join([]string{nameVR, "S", orgid, org}, ":")
	nameV := strings.Join([]string{name, version}, "-")
	nameV = strings.Join([]string{nameV, "S", orgid, org}, ":")

	return nameVR, nameV
}
