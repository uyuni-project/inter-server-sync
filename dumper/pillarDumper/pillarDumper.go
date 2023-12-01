// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package pillarDumper

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var serverDataDir = "/srv/susemanager/pillar_data/"
var replacePattern = "{SERVER_FQDN}"

func DumpImagePillars(outputDir string, orgIds []uint, serverConfig string) {
	log.Debug().Msgf("Dumping pillars to %s", outputDir)
	fqdn := utils.GetCurrentServerFQDN(serverConfig)

	sourceDir := filepath.Join(serverDataDir, "images")
	orgDir, err := os.Open(sourceDir)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer orgDir.Close()
	orgDirInfo, err := orgDir.ReadDir(-1)

	// If orgIds is empty, set it to 0 so all orgs would be exported
	if len(orgIds) == 0 {
		orgIds = []uint{0}
	}

	for _, org := range orgDirInfo {
		for _, orgId := range orgIds {
			if org.Type().IsDir() && (orgId == 0 || org.Name() == fmt.Sprintf("org%d", orgId)) {
				DumpPillars(path.Join(sourceDir, org.Name()), path.Join(outputDir, org.Name()), fqdn, replacePattern)
			}
		}

	}
}

func DumpPillars(sourceDir, outputDir, sourceFQDN, targetFQDN string) {
	log.Trace().Msgf("Pillar dump for %s, replacing FQDN %s", sourceDir, sourceFQDN)

	pillarDir, err := os.Open(sourceDir)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer pillarDir.Close()
	pillarDirInfo, err := pillarDir.ReadDir(-1)

	for _, pillar := range pillarDirInfo {
		if pillar.Type().IsRegular() {
			pillarFilePath := path.Join(sourceDir, pillar.Name())
			pillarTargetPath := path.Join(outputDir, pillar.Name())
			log.Trace().Msgf("Parsing and copying pillar from %s to %s", pillarFilePath, pillarTargetPath)

			_, err := dumper.ModifyCopy(pillarFilePath,
				pillarTargetPath,
				sourceFQDN, targetFQDN)
			if err != nil {
				log.Fatal().Err(err)
			}
			os.Chmod(pillarTargetPath, 0640)
			cmd := exec.Command("chown", "salt:susemanager", pillarTargetPath)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				log.Fatal().Err(err).Msg("Error processing image pillar files")
			}
		}
	}
}

// 4.2 and older stores pillars in files
// image export replaces hostnames in image pillars, we need to replace them to correct SUMA on import
func ImportImagePillars(sourceDir string, fqdn string) {
	log.Debug().Msgf("Importing image pillars from %s", sourceDir)
	orgDir, err := os.Open(sourceDir)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer orgDir.Close()
	orgDirInfo, err := orgDir.ReadDir(-1)

	for _, org := range orgDirInfo {
		if org.Type().IsDir() {
			targetDir := path.Join(serverDataDir, "images", org.Name())
			DumpPillars(path.Join(sourceDir, org.Name()), targetDir, replacePattern, fqdn)

			cmd := exec.Command("chown", "salt:susemanager", targetDir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				log.Fatal().Err(err).Msg("Error importing image pillar files")

			}
		}
	}
}

// 4.3 and newer stores pillars in database
// image export replaces hostnames in image pillars, we need to replace them to correct SUMA on import
func UpdateImagePillars(serverConfig string) {
	fqdn := utils.GetCurrentServerFQDN(serverConfig)

	checkQuery := "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'susesaltpillar')"
	db := schemareader.GetDBconnection(serverConfig)
	rows, err := db.Query(checkQuery)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error while executing '%s'", checkQuery)
	}
	if !rows.Next() {
		log.Fatal().Msgf("No return on pillar database table check")
	}
	var hasPillars bool
	err = rows.Scan(&hasPillars)
	if err != nil {
		log.Fatal().Err(err).Msgf("Unexpected query result")
	}
	if !hasPillars {
		log.Debug().Msgf("Pillars not backed by database")
		return
	}

	sqlQuery := fmt.Sprintf("UPDATE susesaltpillar SET pillar = REPLACE(pillar::text, '%s', '%s')::jsonb WHERE category LIKE 'Image%%';",
		replacePattern, fqdn)
	log.Trace().Msgf("Updating pillar files using query '%s'", sqlQuery)
	log.Info().Msg("Updating image pillars if needed")
	rows, err = db.Query(sqlQuery)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error updating image pillars")
	}
}
