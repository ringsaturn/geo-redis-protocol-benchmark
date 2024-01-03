package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/redis/go-redis/v9"
	gocitiesjson "github.com/ringsaturn/go-cities.json"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/ratelimit"
)

var (
	countryCode = flag.String("country", "", "Country code")
	coordsOrder = flag.String("coords", "lat,lon", "Coordinates order, `lat,lon` or `lon,lat`")
	redisURI    = flag.String("redis", "localhost:6379", "Redis URI")
	cmdTPL      = flag.String("cmd", "GET", "Command template")
	qps         = flag.Int("qps", 10, "Queries per second")
	threads     = flag.Int("threads", 10, "Number of threads")
	runs        = flag.Int("runs", 1, "Run how many cities in one batch")
	timeout     = flag.Int("timeout", 10, "Timeout in seconds")

	cities []*gocitiesjson.City

	ok, failed, reqFailed int64
	failedStatusCode      = func() map[int]*int64 {
		m := map[int]*int64{}
		for i := 0; i < 600; i++ {
			m[i] = new(int64)
		}
		return m
	}()

	oklatency = []float64{}
	okmutex   sync.Mutex

	failedlatency = []float64{}
	failedmutex   sync.Mutex

	rc *redis.Client
)

func prepare() {
	flag.Parse()

	if *countryCode != "" {
		for _, city := range gocitiesjson.Cities {
			if city.Country == *countryCode {
				cities = append(cities, city)
			}
		}
	} else {
		cities = gocitiesjson.Cities
	}

	log.Println("countryCode", *countryCode)
	log.Println("coordsOrder", *coordsOrder)
	log.Println("redisURI", *redisURI)
	log.Println("cmdTPL", *cmdTPL)
	log.Println("qps", *qps)
	log.Println("threads", *threads)
	log.Println("runs", *runs)
	log.Println("timeout", *timeout)

	rc = redis.NewClient(&redis.Options{
		Addr: *redisURI,
	})
}

func ReportFailedStatus() {
	for code, count := range failedStatusCode {
		if *count != 0 {
			fmt.Printf("http:%v, count:%v\n", code, *count)
		}
	}
}

func ReportLatencyHistogram(latency []float64) {
	hist := histogram.Hist(9, latency)
	err := histogram.Fprintf(os.Stdout, hist, histogram.Linear(5), func(v float64) string {
		_v := math.Round(v*1000) / 1000
		return time.Duration(_v).String()
	})
	if err != nil {
		panic(err)
	}
}

func Req(ctx context.Context, city *gocitiesjson.City) {
	start := time.Now()

	var cmdResult *redis.Cmd
	if *coordsOrder == "lat,lon" {
		cmdResult = rc.Do(ctx, cmdTPL, city.Lat, city.Lng)
	} else {
		cmdResult = rc.Do(ctx, cmdTPL, city.Lng, city.Lat)
	}

	duration := float64(time.Since(start).Nanoseconds())

	if err := cmdResult.Err(); err != nil {
		atomic.AddInt64(&reqFailed, 1)
		failedmutex.Lock()
		failedlatency = append(failedlatency, duration)
		failedmutex.Unlock()
		return
	}

	atomic.AddInt64(&ok, 1)
	okmutex.Lock()
	oklatency = append(oklatency, duration)
	okmutex.Unlock()
}

func RunConcPool(rootCtx context.Context) {
	rl := ratelimit.New(*qps)
	p := pool.New().WithMaxGoroutines(*threads).WithContext(rootCtx)
	for i := 0; i < *runs; i++ {
		p.Go(
			func(reqctx context.Context) error {
				rl.Take()
				ctx, cancel := context.WithTimeout(reqctx, time.Duration(*timeout)*time.Second)
				defer cancel()

				city := cities[rand.Intn(len(cities))]

				Req(ctx, city)
				return nil
			})
	}
	p.Wait()
}

func main() {
	start := time.Now()
	prepare()
	ctx := context.Background()
	RunConcPool(ctx)
	fmt.Println("total time", time.Since(start))
	fmt.Println("ok", ok)
	fmt.Println("failed", failed)
	fmt.Println("reqFailed", reqFailed)

	ReportFailedStatus()
	fmt.Println()

	fmt.Println("oklatency:")
	ReportLatencyHistogram(oklatency)
	fmt.Println()

	fmt.Println("failedlatency:")
	ReportLatencyHistogram(failedlatency)
	fmt.Println()

	fmt.Println("======================================")
	fmt.Println()
}
