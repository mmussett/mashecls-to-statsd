package main

import (
	"encoding/json"
	"flag"
	"github.com/gorilla/websocket"
	"github.com/quipo/statsd"
	"github.com/op/go-logging"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
	"fmt"
)

const (
	statsdHost         = "0.0.0.0:8125"
)

var (

	statsdClientService *statsd.StatsdClient
	statsdClientDeveloper *statsd.StatsdClient

	statsdBufferService *statsd.StatsdBuffer
	statsdBufferDeveloper *statsd.StatsdBuffer

	interval = time.Second * 5

	log    = logging.MustGetLogger("mdre")
	format = logging.MustStringFormatter(`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.5s} %{color:reset} %{message}`)


)

type ECLS struct {
	Data []struct {
		APIKey                        string `json:"api_key"`
		APIMethodName                 string `json:"api_method_name"`
		Bytes                         string `json:"bytes"`
		CacheHit                      string `json:"cache_hit"`
		ClientTransferTime            string `json:"client_transfer_time"`
		ConnectTime                   string `json:"connect_time"`
		EndpointName                  string `json:"endpoint_name"`
		HTTPMethod                    string `json:"http_method"`
		HTTPStatusCode                string `json:"http_status_code"`
		HTTPVersion                   string `json:"http_version"`
		OauthAccessToken              string `json:"oauth_access_token"`
		PackageName                   string `json:"package_name"`
		PackageUUID                   string `json:"package_uuid"`
		PlanName                      string `json:"plan_name"`
		PlanUUID                      string `json:"plan_uuid"`
		PreTransferTime               string `json:"pre_transfer_time"`
		QPSThrottleValue              string `json:"qps_throttle_value"`
		QuotaValue                    string `json:"quota_value"`
		Referrer                      string `json:"referrer"`
		RemoteTotalTime               string `json:"remote_total_time"`
		RequestHostName               string `json:"request_host_name"`
		RequestID                     string `json:"request_id"`
		RequestTime                   string `json:"request_time"`
		RequestUUID                   string `json:"request_uuid"`
		ResponseString                string `json:"response_string"`
		ServiceDefinitionEndpointUUID string `json:"service_definition_endpoint_uuid"`
		ServiceID                     string `json:"service_id"`
		ServiceName                   string `json:"service_name"`
		SrcIP                         string `json:"src_ip"`
		SslEnabled                    string `json:"ssl_enabled"`
		TotalRequestExecTime          string `json:"total_request_exec_time"`
		TrafficManager                string `json:"traffic_manager"`
		TrafficManagerErrorCode       string `json:"traffic_manager_error_code"`
		URI                           string `json:"uri"`
		UserAgent                     string `json:"user_agent"`
	} `json:"data"`
}

func init() {

	statsdClientService = statsd.NewStatsdClient(statsdHost, "mashery.service")
	err := statsdClientService.CreateSocket()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	statsdClientDeveloper = statsd.NewStatsdClient(statsdHost, "mashery.developer")
	err = statsdClientDeveloper.CreateSocket()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	statsdBufferService = statsd.NewStatsdBuffer(interval, statsdClientService)
	statsdBufferDeveloper = statsd.NewStatsdBuffer(interval, statsdClientDeveloper)
}


func handleEclsEvent(e ECLS) {


	var statName string
	var serviceName string = e.Data[0].ServiceName
	var packageName string = e.Data[0].PackageName
	var planName string = e.Data[0].PlanName
	var endpointName string = e.Data[0].EndpointName
	var methodName string = e.Data[0].APIMethodName
	var apiKey string = e.Data[0].APIKey
	var httpStatusCode string = e.Data[0].HTTPStatusCode
	var responseString string = e.Data[0].ResponseString
	var httpMethod = e.Data[0].HTTPMethod


	if len(packageName) == 0 {
		packageName = "-"
	}

	if len(planName) == 0 {
		planName = "-"
	}

	if len(methodName) == 0 {
		methodName = "-"
	}


	bytes, _ := strconv.ParseInt(e.Data[0].Bytes, 10, 64)
	f, _ := strconv.ParseFloat(e.Data[0].TotalRequestExecTime, 64)
	totalReqExecTime := int64(f * 1000)

	// mashery.service.<<SERVICENAME>>
	statName = fmt.Sprintf(".%s.%s.%s.%s.%s.%s",serviceName,packageName,planName,endpointName,methodName,httpMethod)
	statsdBufferService.Absolute(statName+".bytes", bytes)
	statsdBufferService.Incr(statName+".status_code."+httpStatusCode,1)
	statsdBufferService.Incr(statName+".response_string."+responseString,1)
	statsdBufferService.Timing(statName+".total_request_exec_time", totalReqExecTime)


	// mashery.developer.<<API KEY>>
	statName = fmt.Sprintf(".%s",apiKey)
	statsdBufferDeveloper.Absolute(statName+".bytes", bytes)
	statsdBufferDeveloper.Incr(statName+".status_code."+httpStatusCode,1)
	statsdBufferDeveloper.Incr(statName+".response_string."+responseString,1)
	statsdBufferDeveloper.Timing(statName+".total_request_exec_time", totalReqExecTime)


	return
}

func main() {

	defer statsdClientService.Close()
	defer statsdClientDeveloper.Close()

	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Println("\n  Usage: mashecls-to-statsd <<ws:// or wss:// URL>>")
		os.Exit(-1)
	}

	addr := os.Args[len(os.Args)-1]
	if !strings.HasPrefix(addr, "ws://") && !strings.HasPrefix(addr, "wss://") {
		addr = "ws://" + addr
	}

	log.Info("connecting to: ", addr)
	c, resp, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		log.Error("handshake failed with status: ", resp.StatusCode)
		os.Exit(-1)
	}

	defer c.Close()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Error("read:", err)
				close(interrupt)
				return
			}

			log.Info(fmt.Sprintf("%s\n", message))

			var e ECLS
			err = json.Unmarshal(message, &e)
			if err != nil {
				log.Error("unmarshal:", err)
			}

			handleEclsEvent(e)

		}
	}()

	// Handle program interrupt
	select {
	case <-interrupt:
		c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.Close()
		os.Exit(0)
	}

}
