package main

type JSONInput struct {
	Time     string `json:"time"`
	Low      int    `json:"low"`
	Moderate int    `json:"moderate"`
	High     int    `json:"high"`
}

type JSONMap []map[string]string
