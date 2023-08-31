package main

import (
	"context"
	"fmt"
	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/batchutils"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/config/logger"
	. "github.com/momentohq/client-sdk-go/momento"
	"math"
	"sort"
	"time"
)

var (
	ctx       context.Context
	client    CacheClient
	cacheName string
	keys      []Value
)

func main() {
	ctx = context.Background()
	cacheName = "test_batch_get"
	credentialProvider, err := auth.FromString("eyJhcGlfa2V5IjoiZXlKaGJHY2lPaUpJVXpJMU5pSjkuZXlKemRXSWlPaUp3Y21GMGFXdEFiVzl0Wlc1MGIyaHhMbU52YlNJc0luWmxjaUk2TVN3aWNDSTZJaUo5Lmk3Nkg0amwwSzlGV2c4dVhpeE9aMkZkcEdOczdHVnc0Y3lvWkd5MEFvV1UiLCJlbmRwb2ludCI6ImNlbGwtYWxwaGEtZGV2LnByZXByb2QuYS5tb21lbnRvaHEuY29tIn0=")
	if err != nil {
		panic(err)
	}
	client, err = NewCacheClient(
		config.LaptopLatestWithLogger(logger.NewNoopMomentoLoggerFactory()),
		credentialProvider,
		time.Hour*60,
	)

	_, err = client.CreateCache(ctx, &CreateCacheRequest{CacheName: cacheName})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		key := String(fmt.Sprintf("key%d", i))
		keys = append(keys, key)
		//_, err := client.Set(ctx, &SetRequest{
		//	CacheName: cacheName,
		//	Key:       key,
		//	Value:     String(fmt.Sprintf("val%d", i)),
		//})
		//if err != nil {
		//	panic(err)
		//}
	}

	maxConcurrentGetsValues := []int{5, 10, 20, 25, 50, 100}

	for _, maxConcurrentGets := range maxConcurrentGetsValues {
		fmt.Printf("=== Stats for maxConcurrentGets: %d ===\n", maxConcurrentGets)
		calculateLatencyStats(maxConcurrentGets)
	}
}

func calculateLatencyStats(maxConcurrentGets int) {
	var elapsedTimes []float64
	errorCount := 0

	for j := 0; j < 100; j++ {
		startTime := time.Now()
		_, errors := batchutils.BatchGet(ctx, &batchutils.BatchGetRequest{
			Client:            client,
			CacheName:         cacheName,
			Keys:              keys,
			MaxConcurrentGets: maxConcurrentGets,
		})
		if errors != nil {
			for _, err := range errors.Errors() {
				fmt.Printf("Error is " + err.Error())
				errorCount++
			}
		}

		elapsed := time.Since(startTime).Seconds() * 1000 // Convert to milliseconds
		elapsedTimes = append(elapsedTimes, elapsed)
	}

	// Sort the elapsed times
	sort.Float64s(elapsedTimes)

	// Calculate the average time
	var avg float64
	for _, v := range elapsedTimes {
		avg += v
	}
	avg /= float64(len(elapsedTimes))

	// Calculate percentiles and max
	p90 := percentile(elapsedTimes, 90)
	p99 := percentile(elapsedTimes, 99)
	p999 := percentile(elapsedTimes, 99.9)
	max := percentile(elapsedTimes, 100)

	fmt.Printf("Average: %.2f ms\n", avg)
	fmt.Printf("P90: %.2f ms\n", p90)
	fmt.Printf("P99: %.2f ms\n", p99)
	fmt.Printf("P999: %.2f ms\n", p999)
	fmt.Printf("Max: %.2f ms\n", max)
	fmt.Printf("Error Count: %d\n", errorCount)
}

func percentile(sorted []float64, p float64) float64 {
	index := p / 100 * float64(len(sorted)-1)
	whole := int(math.Floor(index))
	frac := index - float64(whole)

	if whole+1 >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	return sorted[whole]*(1-frac) + sorted[whole+1]*frac
}
