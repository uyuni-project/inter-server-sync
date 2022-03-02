package osImageDumper

import (
	"fmt"
	"os"
	"path"

	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/dumper"
)

var serverDataFolder = "/srv/www/os-images/"

//FIXME: we have no relation from db tables to actial data so for now copy content of serverDataFolder
//func DumpOsImages(db *sql.DB, schemaMetadata map[string]schemareader.Table, data dumper.DataDumper, outputFolder string) {
func DumpOsImages(outputFolder string, orgIds []uint) {
	log.Debug().Msg("Image data dump")

	imagesDir, err := os.Open(serverDataFolder)
	if err != nil {
		log.Fatal().Err(err)
	}
	defer imagesDir.Close()
	orgDirInfo, err := imagesDir.ReadDir(-1)

	if len(orgIds) == 0 {
		orgIds = []uint{0}
	}

	for _, org := range orgDirInfo {
		for _, orgId := range orgIds {
			if org.Type().IsDir() && (orgId == 0 || org.Name() == fmt.Sprint(orgId)) {
				var orgDirPath = path.Join(serverDataFolder, org.Name())
				orgDir, err := os.Open(orgDirPath)
				if err != nil {
					log.Fatal().Err(err)
				}
				defer orgDir.Close()
				orgDirInfo, err := orgDir.ReadDir(-1)

				for _, image := range orgDirInfo {
					if image.Type().IsRegular() {
						var imagePath = path.Join(orgDirPath, image.Name())
						log.Trace().Msgf("Copying image %s", imagePath)
						_, err := dumper.Copy(imagePath, path.Join(outputFolder, org.Name(), image.Name()))
						if err != nil {
							log.Fatal().Err(err)
						}
					}
				}
			}
		}
	}
}
