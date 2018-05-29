package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func main() {
	fdb.MustAPIVersion(510)
	db := fdb.MustOpenDefault()

	metricsDir, err := directory.CreateOrOpen(db, []string{"metrics"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	measuresSS := metricsDir.Sub("measures")

	tags := make(map[string]string)
	fields := make(map[string]interface{})

	StoreMeasurement(db, measuresSS, "cpu", tags, fields, time.Now())

	now := time.Now()
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	to := from.AddDate(0, 0, 1)

	QueryMeasurements(db, measuresSS, "cpu", from, to)
}

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
) {
	// cpu,cpu=cpu0,host=hyperdawn usage_system=0.5
	// SELECT mean("usage_user") FROM "cpu" WHERE ("cpu" = 'cpu-total') AND time >= now() - 6h GROUP BY time(10s) fill(null)

	ms := ts.UnixNano() / 1000000

	fmt.Println("ms:", ms)

	key := ss.Pack(tuple.Tuple{measurement, ms})

	fmt.Println(key)

	t.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

		value := 12345.6
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
