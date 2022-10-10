package main

import (
	"encoding/json"
	"errors"
	"github.com/gobeam/stringy"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	runTimeStamp = time.Now()
)

func main() {
	var originalPositions, updatedPositions Positions

	pflag.String("time.format", "2006-01-02", "time field layout in golang time.Parse format")
	pflag.String("time.field", "time", "field to get the time for")
	pflag.String("positions.file", "positions.yml", "file to keep track of positions")
	pflag.Bool("debug", false, "more logging")
	pflag.String("default.label", "table", "label to set from key if there are no labels in record")
	getConfig()

	log := SetupLog()
	var err error
	originalPositions, err = LoadPositions()
	if err != nil {
		log.Fatal().Err(err).Msg("couldn't load positions")
	}
	updatedPositions, err = LoadPositions()
	if err != nil {
		log.Fatal().Err(err).Msg("couldn't load positions")
	}

	for _, f := range pflag.Args() {
		processFile(log, f, originalPositions, &updatedPositions)
	}

	err = SavePositions(updatedPositions)
	if err != nil {
		log.Fatal().Err(err).Msg("couldn't save positions")
	}

}

func processFile(log zerolog.Logger, inputFileName string, originalPositions Positions, updatedPositions *Positions) {
	jsonFile, err := os.Open(inputFileName)
	client := influxdb2.NewClient("http://localhost:8086", viper.GetString("influx.token"))
	defer client.Close()

	if err != nil {
		log.Fatal().Err(err).Msg("failed to open input")
	}
	defer jsonFile.Close()

	basename := filepath.Base(inputFileName)

	i := strings.IndexByte(basename, '_')
	j := strings.Index(basename, "Z-")

	if i == -1 {
		log.Fatal().Err(err).Str("file", basename).Msg("filename does not have an _")
		return
	}
	if j == -1 {
		log.Fatal().Err(err).Str("file", basename).Msg("filename does not have a timestamp")
		return
	}

	defaultTimeStampStr := basename[i+1 : j+1]
	log.Debug().Str("default_ts", defaultTimeStampStr).Msg("Default Timestamp")
	defaultTimeStamp, err := time.Parse("20060102T150405Z", defaultTimeStampStr)
	if err != nil {
		log.Debug().Err(err).Msg("failed to parse default time stamp, defaulting to run time")
		defaultTimeStamp = runTimeStamp
	}

	basename = basename[:i]
	baseMetricName := to_snake_case(basename)

	filterTS, found := originalPositions.Positions[baseMetricName]
	if !found {
		filterTS = time.Time{}
	}
	maxTs := time.Time{}
	filteredMetrics := 0

	byteValue, _ := ioutil.ReadAll(jsonFile)
	//	log.V(1).Info("Starting to process", "in", inputFileName, "out", outputFileName, "metric", baseMetricName)

	var jsonMap []map[string]interface{}
	json.Unmarshal(byteValue, &jsonMap)

	log.Trace().Int("count", len(jsonMap)).Msg("Read Json lines")

	//	prevTimeStamp, _ := time.Parse("2006-01-02", "2999-01-01")

	timefield := viper.GetString("time.field")
	//	timeformat := viper.GetString("time.format")
	timeformats := viper.GetStringSlice("time.formats")
	numrows := 0
	for _, row := range jsonMap {
		numrows++
		rowTime, found := row[timefield].(string)
		var rowTimeStamp time.Time
		if found {
			succesfullyParsed := false
			for _, timeformat := range timeformats {
				rowTimeStamp, err = time.Parse(timeformat, rowTime)
				if err == nil {
					succesfullyParsed = true
					break
				}
			}
			if !succesfullyParsed {
				log.Error().Err(err).Str("timefield", rowTime).Str("file", basename).Msg("couldn't parse time")
			}

		} else {
			rowTimeStamp = defaultTimeStamp
			log.Trace().Time("runtime", runTimeStamp).Msg("defaulted time")
		}

		// Filter metrics
		if filterTS.After(rowTimeStamp) || filterTS.Equal(rowTimeStamp) {
			filteredMetrics++
			continue
		}
		if maxTs.Before(rowTimeStamp) {
			maxTs = rowTimeStamp
		}

		values := make(map[string]interface{})
		labels := make(map[string]string)

		for k, v := range row {
			switch k {
			case "time":
				// time handled above
			case "count_percent":
				// handle "315 (15.7%)"
				fields := strings.Fields(v.(string))
				if len(fields) == 2 {
					values["count"], err = strconv.ParseFloat(fields[0], 64)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert count_percent to float")
					}
					pc := strings.Trim(fields[1], "(%)")
					values["pc"], err = strconv.ParseFloat(pc, 64)
				} else {
					log.Error().Err(errors.New("not enough fields")).Msg("couldn't parse count_percent field")
				}
			default:
				switch v.(type) {
				case float64:
					values[k] = v.(float64)
				case string:
					labels[k] = v.(string)
				}
			}
		}

		addMetrics(client, values, labels, rowTimeStamp, baseMetricName)
	}
	log.Info().Str("baseMetricName", baseMetricName).Int("numrows", numrows).Msg("Wrote Metrics")
	updatedPos, _ := updatedPositions.Positions[baseMetricName]
	if maxTs.After(updatedPos) {
		updatedPositions.Positions[baseMetricName] = maxTs
	}
}

func to_snake_case(basename string) string {
	baseMetricName := stringy.New(basename).SnakeCase("?", "").ToLower()
	return baseMetricName
}

func addMetrics(client influxdb2.Client, values map[string]interface{}, labels map[string]string, t time.Time, mn string) {

	if len(values) == 0 {
		log.Info().Str("metric", mn).Msg("skipped metrics due to empty values")
		return
	}

	defaultMetricNames := viper.GetStringMapString("default_labelname")
	defaultLabelName, hasDefault := defaultMetricNames[mn]

	writeAPI := client.WriteAPI("solidmon", "is")

	if hasDefault {
		for k, v := range values {
			lbs := make(map[string]string)
			for kk, vv := range labels {
				lbs[kk] = vv
			}
			lbs[defaultLabelName] = k
			vs := map[string]interface{}{
				"value": v,
			}
			p := influxdb2.NewPoint(mn,
				lbs,
				vs,
				t)
			writeAPI.WritePoint(p)
		}
	} else {

		p := influxdb2.NewPoint(mn,
			labels,
			values,
			t)
		writeAPI.WritePoint(p)
	}
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
