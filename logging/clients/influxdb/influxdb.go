package influxdb

import (
	"fmt"

	"os"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/turbosonic/event-hub-sidecar/logging"
)

var (
	influxDBNAME string
	wg           sync.WaitGroup
	bp           client.BatchPoints
)

type influxdbLogger struct {
	client client.Client
}

func (l influxdbLogger) LogInboundEvent(e logging.InboundEvent) error {
	tags := map[string]string{
		"ID":           e.ID,
		"Name":         e.Name,
		"Microservice": e.Microservice,
		"PodName":      e.PodName,
		"Status":       e.Status.String(),
	}
	fields := map[string]interface{}{
		"Duration": e.Duration,
		"Attempts": e.Attempts,
	}
	pt, err := client.NewPoint("inbound", tags, fields, e.Processed)
	if err != nil {
		return err
	}

	wg.Wait()
	bp.AddPoint(pt)

	return nil
}

func (l influxdbLogger) LogOutboundEvent(e logging.OutboundEvent) error {
	tags := map[string]string{
		"ID":           e.ID,
		"Name":         e.Name,
		"Microservice": e.Microservice,
		"PodName":      e.PodName,
		"Status":       e.Status.String(),
	}
	fields := map[string]interface{}{
		"Attempts": e.Attempts,
	}
	pt, err := client.NewPoint("outbound", tags, fields, e.Sent)
	if err != nil {
		return err
	}

	wg.Wait()
	bp.AddPoint(pt)

	return nil
}

func New() influxdbLogger {
	influxDBURL := os.Getenv("LOGGING_INFLUXDB_URL")
	if influxDBURL == "" {
		panic("No LOGGING_INFLUXDB_URL environment variable found")
	}

	influxDBNAME = os.Getenv("LOGGING_INFLUX_DB_NAME")
	if influxDBNAME == "" {
		influxDBNAME = "eventhub"
	}

	// create a client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: influxDBURL,
	})
	if err != nil {
		panic("Error creating InfluxDB Client")
	}

	// need to create the database here, unless it already exists
	q := client.NewQuery("CREATE DATABASE "+influxDBNAME, "", "")
	if response, err := c.Query(q); err != nil && response.Error() != nil {
		fmt.Println(response.Error())
	}

	bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influxDBNAME,
		Precision: "ns",
	})

	logger := influxdbLogger{
		client: c,
	}

	logger.startDataWriter()

	return logger
}

func (influxdb influxdbLogger) startDataWriter() {
	go func() {
		for {
			time.Sleep(time.Second * 5)
			wg.Add(1)
			pointsCount := len(bp.Points())
			if pointsCount > 0 {
				influxdb.client.Write(bp)
				bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  influxDBNAME,
					Precision: "ns",
				})

				fmt.Printf("%d points sent to InfluxDB\n", pointsCount)
			}
			wg.Done()
		}
	}()
}
