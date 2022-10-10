package main

import (
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"os"
)

func SetupLog() zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerologr.NameFieldName = "logger"
	zerologr.NameSeparator = "/"

	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	zl := zerolog.New(os.Stderr)
	zl = zl.With().Caller().Timestamp().Logger()
	return zl
}
