package utils

import (
	"encoding/json"

	"github.com/rs/zerolog/log"
)

func StructToJson(data interface{}) []byte {
	var bt []byte
	var err error
	bt, err = json.Marshal(data)
	if err != nil {
		log.Error().Msg("couldn't unmarshall the request")
	}

	return bt
}
