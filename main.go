package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	cluster "github.com/bsm/sarama-cluster"
)

func main() {

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	brokers := []string{"localhost:9092"}
	group := "measures-1"
	topics := []string{"measures"}
	consumer, err := cluster.NewConsumer(brokers, group, topics, config)

	if err != nil {
		log.Fatal("Couldn't create consumer group.", err)
	}

	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
				// parseValue(msg.Value)
			}
		case <-signals:
			return
		case <-time.After(7 * time.Second):
			fmt.Println("")
		}
	}

	// rand.Seed(time.Now().UTC().UnixNano())

	// fdb.MustAPIVersion(510)
	// db := fdb.MustOpenDefault()

	// metricsDir, err := directory.CreateOrOpen(db, []string{"metrics"}, nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// measuresSS := metricsDir.Sub("measures")

	// tags := make(map[string]string)

	// tags["host"] = "hyperdawn"
	// tags["cpu"] = "cpu0"

	// fields := make(map[string]interface{})
	// // n := randRange(1, 10000)
	// value := rand.Float64() /* * float64(n) */

	// StoreMeasurement(db, measuresSS, "cpu", tags, fields, time.Now(), value)

	// now := time.Now()
	// from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	// to := from.AddDate(0, 0, 1)

	// QueryMeasurements(db, measuresSS, "cpu", from, to)
}

func randRange(min, max int) int {
	return min + rand.Intn(max-min)
}

// func parseValue(v []byte) {
// 	fmt.Println("=================")
// 	str := string(v)
// 	parts := strings.Fields(str)
// 	for _, part := range parts {
// 		fmt.Println(part)
// 	}

// 	p1 := strings.split(parts[0], ",")

// 	measurement := p1[0]

// 	fmt.Println("=================")
// }

// NewMeasurementKeyRange blah blah
func NewMeasurementKeyRange(ss subspace.Subspace, measurement string, from, to time.Time) fdb.KeyRange {

	fmt.Println("From:", from)
	fmt.Println("To:  ", to)

	fromMs := from.UnixNano() / 1000000
	toMs := to.UnixNano() / 1000000

	fmt.Println("fromMs:", fromMs)
	fmt.Println("toMs:  ", toMs)

	fromKey := ss.Pack(tuple.Tuple{measurement, fromMs})
	toKey := ss.Pack(tuple.Tuple{measurement, toMs})

	fmt.Println("fromKey:", fromKey)
	fmt.Println("toKey:  ", toKey)

	return fdb.KeyRange{fromKey, toKey}
}

// StoreMeasurement blah blah
func StoreMeasurement(
	t fdb.Transactor,
	ss subspace.Subspace,
	measurement string,
	tags map[string]string,
	fields map[string]interface{},
	ts time.Time,
	value float64,
) {
	// cpu,cpu=cpu0,host=hyperdawn usage_system=0.5
	// SELECT mean("usage_user") FROM "cpu" WHERE ("cpu" = 'cpu-total') AND time >= now() - 6h GROUP BY time(10s) fill(null)

	ms := ts.UnixNano() / 1000000

	fmt.Println("ms:", ms)

	key := ss.Pack(tuple.Tuple{measurement, ms})

	fmt.Println(key)

	t.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

		tr.Set(key, []byte(strconv.FormatFloat(value, 'f', -1, 64)))

		return
	})

}

// QueryMeasurements blah blah
func QueryMeasurements(t fdb.Transactor, ss subspace.Subspace, measurement string, from, to time.Time) (ac []string, err error) {

	keyRange := NewMeasurementKeyRange(ss, measurement, from, to)

	t.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {

		ri := rtr.GetRange(keyRange, fdb.RangeOptions{}).Iterator()
		for ri.Advance() {
			fmt.Println(">>>>>")
			kv := ri.MustGet()
			v, err := strconv.ParseFloat(string(kv.Value), 64)
			if err != nil {
				fmt.Println("Error:", err)
				return nil, err
			}
			if v > 0 {
				t, err := ss.Unpack(kv.Key)
				if err != nil {
					return nil, err
				}
				fmt.Println("key:", t)
				fmt.Println("val:", v)
			}
		}
		return nil, nil
	})

	return
}
