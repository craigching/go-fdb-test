package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"time"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/influxdata/influxdb/models"
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

	fdb.MustAPIVersion(510)
	db := fdb.MustOpenDefault()

	metricsDir, err := directory.CreateOrOpen(db, []string{"metrics-20180601-1"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	measuresSS := metricsDir.Sub("measures")

	now := time.Now()
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	to := from.AddDate(0, 0, 1)

	// cpu,cpu=cpu0,host=8c8590ad83d9 usage_irq=0,usage_steal=0,usage_user=7.592407592407592,usage_system=5.694305694305695,usage_iowait=0,usage_guest=0,usage_guest_nice=0,usage_idle=86.7132867132867,usage_nice=0,usage_softirq=0

	QueryMeasurements(db, measuresSS, "cpu", "cpu=cpu0,host=8c8590ad83d9", "usage_system", from, to)

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
				parseValue(db, measuresSS, msg.Value)
			}
		case <-signals:
			return
		case <-time.After(7 * time.Second):
			fmt.Println("")
		}
	}

	// rand.Seed(time.Now().UTC().UnixNano())


	// tags := make(map[string]string)

	// tags["host"] = "hyperdawn"
	// tags["cpu"] = "cpu0"

	// fields := make(map[string]interface{})
	// // n := randRange(1, 10000)
	// value := rand.Float64() /* * float64(n) */

	// StoreMeasurement(db, measuresSS, "cpu", tags, fields, time.Now(), value)
}

func randRange(min, max int) int {
	return min + rand.Intn(max-min)
}

func parseValue(t fdb.Transactor, ss subspace.Subspace, v []byte) {
	fmt.Println("=================")

	points, err := models.ParsePoints(v)

	if err != nil {
		fmt.Println("ERROR:", err)
	}

	for _, point := range points {
		fmt.Printf("%+v\n", point.Tags())
		fmt.Println("Tags type:", reflect.TypeOf(point.Tags()).String())
		// fmt.Printf("%+v\n", point.Fields())
		// fmt.Println("Fields type:", reflect.TypeOf(point.Fields()).String())

		name := point.Name()

		ts := point.UnixNano() / 1000000

		fmt.Println("Time:", point.Time())

		entries := make([]string, 0)
		// entries = append(entries, string(name))
		for _, tag := range point.Tags() {
			entry := fmt.Sprintf("%s=%s", tag.Key, tag.Value)
			entries = append(entries, entry)
		}

		tagsKey := strings.Join(entries, ",")

		fmt.Println("tagsKey:", tagsKey)

		fi := point.FieldIterator()
		for fi.Next() {
			fieldName := string(fi.FieldKey())

			// ([measurement, tagsString, field name, timestamp], value)

			fmt.Printf("%s %s %s %d\n", name, tagsKey, fieldName, ts)
		
			key := ss.Pack(tuple.Tuple{name, tagsKey, fieldName, ts})
			tv, err := getValue(fi)

			fmt.Println("tv:", tv)

			if err != nil {
				fmt.Println("ERROR:", err)
			}

			value := ss.Pack(tv)
			
			fmt.Println("key:", key)
			fmt.Println("value:", value)

			ret, err := t.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

				tr.Set(key, value)

				return
			})

			if err != nil {
				fmt.Println("ERROR inserting data:", err)
			}

			fmt.Println("ret:", ret)
		}
	}

	fmt.Println("=================")
}

func getValue(fi models.FieldIterator) (tuple.Tuple, error) {
	switch fi.Type() {
	case models.Integer:
		value, _ := fi.IntegerValue()
		fmt.Printf("%d\n", value)
		return tuple.Tuple{int64(models.Integer), value}, nil
	case models.Float:
		value, _ := fi.FloatValue()
		// fmt.Printf("%s=%f\n", fieldName, value)
		return tuple.Tuple{int64(models.Float), value}, nil
	case models.Boolean:
		value, _ := fi.BooleanValue()
		// fmt.Printf("%s=%b\n", fieldName, value)
		return tuple.Tuple{int64(models.Boolean), value}, nil
	case models.String:
		value := fi.StringValue()
		// fmt.Printf("%s=%s\n", fieldName, value)
		return tuple.Tuple{int64(models.String), value}, nil
	}

	return tuple.Tuple{}, errors.New("Invalid data type")
}

// NewMeasurementKeyRange blah blah
func NewMeasurementKeyRange(
	ss subspace.Subspace,
	measurement string,
	tagStr string,
	fieldName string,
	from, to time.Time) fdb.KeyRange {

	fmt.Println("From:", from)
	fmt.Println("To:  ", to)

	fromMs := from.UnixNano() / 1000000
	toMs := to.UnixNano() / 1000000

	fmt.Println("fromMs:", fromMs)
	fmt.Println("toMs:  ", toMs)

	fromKey := ss.Pack(tuple.Tuple{measurement, tagStr, fieldName, fromMs})
	toKey := ss.Pack(tuple.Tuple{measurement, tagStr, fieldName, toMs})

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
func QueryMeasurements(
	t fdb.Transactor,
	ss subspace.Subspace,
	measurement string,
	tagString string,
	fieldName string,
	from, to time.Time) (ac []string, err error) {

	keyRange := NewMeasurementKeyRange(ss, measurement, tagString, fieldName, from, to)

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
			t, err := ss.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			fmt.Println("key:", t)
			fmt.Println("val:", v)
		}
		return nil, nil
	})

	return
}
