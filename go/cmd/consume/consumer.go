package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"

	"log"
	"net/http"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/segmentio/encoding/json"
	"gopkg.in/ini.v1"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// Command line parameters
type CmdArgs struct {
	Config string `arg:"required,-c,--config" help:"The configuration file"`
}

// Configuration file parameters
type RabbitMqConf struct {
	User     string `ini:"user"`
	Password string `ini:"password"`
	Server   string `ini:"server"`
	AmqpPort int    `ini:"amqp_port"`
	HttpPort int    `ini:"http_port"`
	Vhost    string `ini:"vhost"`
	Exchange string `ini:"exchange"`
}

type SplunkConf struct {
	Url   string `ini:"url"`
	Token string `ini:"token"`
}

type Config struct {
	Rabbit  RabbitMqConf
	Splunk  SplunkConf
	Threads int `ini:"threads"`
}

type ProcessResult struct {
	sessionCount int64
}

func GetConfig() *Config {
	// Parse command line arguments to find configuration file
	cmdArgs := &CmdArgs{}
	arg.MustParse(cmdArgs)

	// Now load the configuration file and populate the structures
	config := &Config{}
	iniCfg, err := ini.Load(cmdArgs.Config)
	//cfg, err := ini.Load(cmdArgs.Config)
	failOnError(err, "Failed to parse config file: "+cmdArgs.Config)
	err = iniCfg.Section("general").StrictMapTo(config)
	failOnError(err, "Failed to parse common params: "+cmdArgs.Config)
	err = iniCfg.Section("rabbitmq").StrictMapTo(&config.Rabbit)
	failOnError(err, "Failed to parse rabbitmq params: "+cmdArgs.Config)
	err = iniCfg.Section("splunk").StrictMapTo(&config.Splunk)
	failOnError(err, "Failed to parse splunk params: "+cmdArgs.Config)
	//fmt.Printf("Config: %v\n", config)

	return config
}

type RabbitMq struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	msgChan <-chan amqp.Delivery
}

func AddVhost(config *RabbitMqConf) error {

	// curl -i -u guest:guest -H "content-type:application/json" -XPUT http://localhost:15672/api/vhosts/dx

	url := fmt.Sprintf("http://%v:%v/api/vhosts/%v", config.Server, config.HttpPort, config.Vhost)
	log.Printf("Adding vhost: %s", url)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}

	req.SetBasicAuth(config.User, config.Password)
	req.Header.Add("content-type", "applicastion/json")

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// 201 = Created - happens when created
	// 204 = Success No Data - happens if already created
	if response.StatusCode != 201 && response.StatusCode != 204 {
		return fmt.Errorf("Error setting rabbitmq vhost: %v", response.Status)
	}
	return nil
}

func NewRabbitMq(config *RabbitMqConf, threads int) *RabbitMq {
	// Create a new virtual host, so we can login
	err := AddVhost(config)
	failOnError(err, "Failed to create Rabbit MQ vhost")

	connStr := fmt.Sprintf("amqp://%v:%v@%v:%v/%v",
		config.User, config.Password, config.Server, config.AmqpPort, config.Vhost)
	log.Printf(fmt.Sprintf("Connecting to RabbitMQ: %v", connStr))
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	// Limit the number of unacknowledged messages that can be sent to the consumer.
	// If the prefetch count isn't set, RabbitMQ will continue to send
	// data to the RabbitMQ consumer library, which will buffer it. That will
	// cause the consumer application to continue to consume memory if it can't
	// keep up. It's better to let the messages stay queued in RabbitMQ, where
	// it will be obvious on the RabbitMQ GUI that the consumer isn't keeping
	// up.
	ch.Qos(
		threads*2, // prefetch count
		0,         // prefetch size
		false,     // global
	)

	exchange := config.Exchange
	queue := exchange + "_q"
	// Create the exchange to receive messages
	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // kind
		true,     // durable
		false,    // auto delete
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to create exchange")

	// Create the Queue to publish messages
	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Create a binding to connect the exchange to the Queue
	err = ch.QueueBind(
		queue,    // name
		"",       // key
		exchange, // exchange
		false,    // nowait
		nil,      // arguments
	)
	failOnError(err, "Failed to bind to exchange")

	msgChan, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Printf(fmt.Sprintf("Connected to %v, exchange = %v, queue = %v\n", connStr, exchange, queue))
	return &RabbitMq{conn: conn, ch: ch, msgChan: msgChan}
}

func (p *RabbitMq) MsgChan() <-chan amqp.Delivery {
	return p.msgChan
}

func (p *RabbitMq) Close() error {

	err := p.ch.Close()
	if e2 := p.conn.Close(); e2 != nil && err == nil {
		err = e2
	}

	return err
}

type Splunk struct {
	url    string
	token  string
	client *http.Client
}

func NewSplunk(config *SplunkConf, maxConnsPerHost int) *Splunk {
	tr := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: maxConnsPerHost,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, // Insecure because we don't have certs correct for our Splunk VM
	}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	return &Splunk{url: config.Url, token: config.Token, client: client}
}

func (p *Splunk) addEvent(builder *bytes.Buffer, json_data map[string]interface{}) error {
	// See https://docs.splunk.com/Documentation/Splunk/8.2.6/Data/HECExamples
	// Splunk requires event data to be wrapped in an "event" element.  It also
	// allows multiple events in a single message. When receiving metadata from
	// Security Analytics, the events will be wrapped in a JSON list. Each item
	// in the list needs to be wrapped in an "event" tag before being sent to
	// splunk.
	// Check if the JSON was an single object or a list
	event := make(map[string]interface{})
	event["event"] = json_data
	// Convert to JSON string
	marshalled, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Add to final Splunk message
	builder.Write(marshalled)
	return nil
}

func (p *Splunk) convert(json_bytes []byte) ([]byte, int64, error) {
	// Parse the JSON to figure out if it's a list or single object
	var parsedJson interface{}
	sessionCount := int64(0)
	err := json.Unmarshal(json_bytes, &parsedJson)
	if err != nil {
		log.Printf("Failed to Unmarshal: %s", string(json_bytes))
		return nil, sessionCount, err
	}
	builder := bytes.Buffer{}
	switch v := parsedJson.(type) {
	case map[string]interface{}: // JSON object
		err := p.addEvent(&builder, v)
		sessionCount = 1
		if err != nil {
			return nil, sessionCount, err
		}
	case []interface{}: // JSON array
		// Loop through array and add event to each item
		sessionCount = int64(len(v))
		for _, elem := range v {
			switch v := elem.(type) {
			// Only a nested JSON object is valid
			case map[string]interface{}: // JSON object
				err := p.addEvent(&builder, v)
				if err != nil {
					return nil, sessionCount, err
				}
			default:
				return nil, sessionCount, fmt.Errorf("Unexpected nested interface type: %T", elem)
			}
		}
	default:
		return nil, sessionCount, fmt.Errorf("Unexpected nested interface type: %T", parsedJson)
	}
	return builder.Bytes(), sessionCount, nil
}

func (p *Splunk) Send(json_bytes []byte) (int64, error) {

	// Convert the json data to a format that Splunk wants
	convertedBytes, sessionCount, err := p.convert(json_bytes)
	//return session_count, err

	if err != nil {
		return sessionCount, err
	}
	req, err := http.NewRequest("POST", p.url, bytes.NewReader(convertedBytes))
	if err != nil {
		return sessionCount, err
	}

	req.Header.Add("Authorization", "Splunk "+p.token)
	response, err := p.client.Do(req)
	if err != nil {
		return sessionCount, err
	}
	defer response.Body.Close()
	// Always consume the response to enable connection sharing
	b, err := io.ReadAll(response.Body)
	if err == nil && response.Status != "200 OK" {
		responseStr := string(b)
		log.Printf("Splunk Response: %s", responseStr)
	}
	return sessionCount, nil
}

func ProcessMessages(rabbit *RabbitMq, splunk *Splunk, resultChan chan ProcessResult) error {
	sessionCount := int64(0)
	var err error = nil
	for delivery := range rabbit.MsgChan() {
		//log.Printf("Received a message. Content type: %s", d.Body)
		if delivery.ContentEncoding == "gzip" {
			// Decompress the body
			reader := bytes.NewReader(delivery.Body)
			gzreader, err := gzip.NewReader(reader)
			if err != nil {
				log.Printf("Error creating gzip reader: %v", err)
			} else {
				output, err := io.ReadAll(gzreader)
				if err != nil {
					return fmt.Errorf("Error decoding gzip content: %w", err)
				} else {
					//fmt.Println(string(output))
					sessionCount, err = splunk.Send(output)
					if err != nil {
						return fmt.Errorf("Error sending data to Splunk: %w", err)
					}
				}
			}
		} else {
			// Data isn't compressed, so send directly
			sessionCount, err = splunk.Send(delivery.Body)
			if err != nil {
				return fmt.Errorf("Error sending data to Splunk: %w", err)
			}
		}

		delivery.Ack(false)
		// Send back the results to the main thread
		resultChan <- ProcessResult{sessionCount}
	}

	return nil
}

func ForwardData(rabbit *RabbitMq, splunk *Splunk, threads int) {
	totalMsgCount := int64(0)
	lastTotalMsgCount := int64(0)
	totalSessionCount := int64(0)
	lastSessionCount := int64(0)
	lastTime := time.Now().Unix()
	// Create a semaphore channel to limit goroutine concurrency
	resultChan := make(chan ProcessResult)
	// Populate channel with zero so that blocking only occurs after all the threads are running
	//log.Printf("Received a message. Content type: %s, Content Encoding: %s", d.ContentType, d.ContentEncoding)
	// fmt.Print("G")
	for i := 0; i < threads; i++ {
		go ProcessMessages(rabbit, splunk, resultChan)
	}
	// Use up one of the semaphores to process the message
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	for result := range resultChan {
		totalSessionCount += result.sessionCount
		if result.sessionCount != 100 {
			log.Printf("%v Sessions", result.sessionCount)
		}
		totalMsgCount += 1
		now := time.Now().Unix()
		deltaTime := now - lastTime

		if deltaTime >= 5 {
			msgPerSecond := (totalMsgCount - lastTotalMsgCount) / deltaTime
			sessionsPerSecond := (totalSessionCount - lastSessionCount) / deltaTime
			fmt.Println()
			log.Printf("[go] Total Message: %v Messages/Second: %v, Sessions/Second: %v",
				totalMsgCount, msgPerSecond, sessionsPerSecond)
			lastTime = now
			lastTotalMsgCount = totalMsgCount
			lastSessionCount = totalSessionCount
		}
	}
}

func main() {
	// Get the configuration values
	config := GetConfig()
	// Create connection to Rabbit MQ
	rabbit := NewRabbitMq(&config.Rabbit, config.Threads)
	defer rabbit.Close()
	// Create connection to Splunk
	splunk := NewSplunk(&config.Splunk, config.Threads)

	// Start forwarding data from Rabbit MQ to Splunk
	ForwardData(rabbit, splunk, config.Threads)
}
