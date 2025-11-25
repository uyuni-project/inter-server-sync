// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package pillarDumper

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/schemareader"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var replacePattern = "{SERVER_FQDN}"

// image export replaces hostnames in image pillars, we need to replace them to correct SUMA on import
func UpdateImagePillars(serverConfig string) {
	fqdn, err := utils.GetCurrentServerFQDN(serverConfig)
	if err != nil {
		log.Error().Msgf("FQDN of server not found, images pillar will not be updated")
		return
	}

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
	_, err = db.Query(sqlQuery)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error updating image pillars")
	}
}
