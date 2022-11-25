package utils

import "github.com/rs/zerolog/log"

func FailOnError(cmp string, err error) {
	if err != nil {
		log.Error().Msgf("%s: %s", cmp, err.Error())
	}
}

func LogWithInfo(cmp, msg string) {

	log.Info().Msgf("%s, %s", cmp, msg)

}
