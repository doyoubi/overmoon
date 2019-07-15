package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/doyoubi/overmoon/src/service"
)

func main() {
	err := service.RunBrokerService()
	log.Infof("service exited %v", err)
}
