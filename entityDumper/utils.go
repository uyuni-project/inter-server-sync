// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package entityDumper

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/utils"
)

func ValidateExportFolder(outputFolderAbs string) {
	ValidateExistingFolder(outputFolderAbs)
	outputFolder, _ := os.Open(outputFolderAbs)
	defer outputFolder.Close()
	_, errEmpty := outputFolder.Readdirnames(1) // Or f.Readdir(1)
	if errEmpty != io.EOF {
		log.Fatal().Msg(fmt.Sprintf("Export location is not empty: %s", outputFolderAbs))
	}
}

func ValidateExistingFolder(outputFolderAbs string) {
	err := utils.FolderExists(outputFolderAbs)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(outputFolderAbs, 0755)
			if err != nil {
				log.Fatal().Err(err).Msg("Error creating directory")
			}
		} else {
			log.Fatal().Err(err).Msg("Error getting output folder")
		}
	}
}
