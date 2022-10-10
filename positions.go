package main

import (
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Positions struct {
	Positions map[string]time.Time `yaml:"positions"`
}

func GenTestPositions() string {
	var p = Positions{Positions: make(map[string]time.Time)}
	var b []byte
	p.Positions["hello"] = time.Now()
	p.Positions["world"] = time.Now()
	b, _ = yaml.Marshal(p)
	s := string(b)
	return s
}

func LoadPositions() (Positions, error) {
	var b []byte
	var p = Positions{Positions: make(map[string]time.Time)}
	filename := viper.GetString("positions.file")
	b, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	err = yaml.Unmarshal(b, &p)
	if err != nil {
		return p, err
	}
	return p, nil
}

func SavePositions(p Positions) error {
	var b []byte
	filename := viper.GetString("positions.file")
	b, err := yaml.Marshal(p)
	if err != nil {
		return err
	}
	_ = os.Rename(filename, filename+".bak")
	err = os.WriteFile(filename, b, 0644)
	if err != nil {
		return err
	}
	return nil
}
