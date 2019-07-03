package main

import (
	"log"

	"github.com/doyoubi/overmoon/src/service"
)

func main() {
	err := service.RunBrokerService()
	log.Printf("service exited %v", err)
}
