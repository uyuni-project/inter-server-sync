package pillarDumper

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
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

func ImportImagePillars(sourceDir string, serverConfig string) {
	log.Debug().Msgf("Importing image pillars from %s", sourceDir)
	fqdn := utils.GetCurrentServerFQDN(serverConfig)
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
