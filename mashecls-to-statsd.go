package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/websocket"
	"github.com/quipo/statsd"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

const (
	prefix     string        = "ecls"
	statsdHost string        = "10.0.0.5:8125"
	bufferSize time.Duration = 2
)

var (
	host         = flag.String("host", "streaming-api.mashery.com", "ECLS Service Host")
	path         = flag.String("path", "/ecls/subscribe/c47f06e6-2ef8-11e7-93ae-92361f002671/Acme", "ECLS Subscription Path")
	key          = flag.String("key", "vm6uYvgwt6rJDTevfUjZjT8WpEkzuaQmRTD", "API Key")
	statsdClientService *statsd.StatsdClient
	statsdClientDeveloper *statsd.StatsdClient
	statsdClientEndpoint *statsd.StatsdClient
	statsdBufferService *statsd.StatsdBuffer
	statsdBufferDeveloper *statsd.StatsdBuffer
	statsdBufferEndpoint  *statsd.StatsdBuffer
	interval = time.Second * 5
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
	log.Println("In init()")

	statsdClientService = statsd.NewStatsdClient(statsdHost, "mashery.service")
	err := statsdClientService.CreateSocket()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	statsdClientDeveloper = statsd.NewStatsdClient(statsdHost, "mashery.developer")
	err = statsdClientDeveloper.CreateSocket()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}


	statsdClientEndpoint = statsd.NewStatsdClient(statsdHost, "mashery.endpoint")
	err = statsdClientEndpoint.CreateSocket()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	statsdBufferService = statsd.NewStatsdBuffer(interval, statsdClientService)
	statsdBufferEndpoint = statsd.NewStatsdBuffer(interval, statsdClientEndpoint)
	statsdBufferDeveloper = statsd.NewStatsdBuffer(interval, statsdClientDeveloper)
}


func emit(e ECLS) {

	spew.Dump(e)

	var statName string
	var serviceName string = e.Data[0].ServiceName
	var packageName string = e.Data[0].PackageName
	var planName string = e.Data[0].PlanName
	var endpointName string = e.Data[0].EndpointName
	var methodName string = e.Data[0].APIMethodName
	var apiKey string = e.Data[0].APIKey
	var httpStatusCode string = e.Data[0].HTTPStatusCode
	var uri string = e.Data[0].URI
	var httpMethod = e.Data[0].HTTPMethod
	var endpoint = strings.Split(uri,"?")



	//var source_ip string = strings.Replace(e.Data[0].SrcIP,".","-",-1)
	if len(packageName) == 0 {
		return
	}

	if len(planName) == 0 {
		return
	}

	if len(methodName) == 0 {
		methodName = "NA"
	}

	statName = "." + serviceName + "." + packageName + "." + planName + "." + endpointName + "." + methodName

	statsdBufferService.Incr(statName, 1)

	i, err := strconv.ParseInt(e.Data[0].Bytes, 10, 64)
	if err == nil {
		statsdBufferService.Absolute(statName+".bytes", i)
	}

	statsdBufferService.Incr(statName+".status_code."+httpStatusCode,1)

	f, err := strconv.ParseFloat(e.Data[0].TotalRequestExecTime, 64)
	d := int64(f * 1000)
	if err == nil {
		statsdBufferService.Timing(statName+".total_request_exec_time", d)
	}

	statName = ".api_key."+apiKey
	statsdBufferDeveloper.Incr(statName,1)
	statsdBufferDeveloper.Absolute(statName+".bytes", i)
	statsdBufferDeveloper.Timing(statName+".total_request_exec_time", d)



	statName = "."+httpMethod+"."+ endpoint[0]

	statsdBufferEndpoint.Incr(statName,1)
	statsdBufferEndpoint.Absolute(statName+".bytes", i)
	statsdBufferEndpoint.Timing(statName+".total_request_exec_time", d)


	return
}

func main() {

	defer statsdClientService.Close()
	defer statsdClientDeveloper.Close()
	defer statsdClientEndpoint.Close()

	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var rawQuery string = "key="
	rawQuery += *key

	u := url.URL{Scheme: "wss", Host: *host, Path: *path, RawQuery: rawQuery}
	log.Printf("connecting to %s", u.String())

	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	d := websocket.Dialer{TLSClientConfig: tlsConfig, EnableCompression: true}

	c, resp, err := d.Dial(u.String(), nil)
	if err != nil {
		log.Printf("handshake failed with status %d", resp.StatusCode)
		os.Exit(-1)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer c.Close()
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}

			var s string = string(message)
			if strings.HasPrefix(s, "Response To") {
			} else {
				var e ECLS
				err = json.Unmarshal(message, &e)
				if err != nil {
					log.Println("unmarshal:", err)
				}

				emit(e)

			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-interrupt:
			log.Println("interrupt")
			// To cleanly close a connection, a client should send a close
			// frame and wait for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			c.Close()
			return
		}
	}
}