package resque

import (
	"strconv"
	"strings"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/kavu/go-resque"
	"github.com/kavu/go-resque/driver"
)

const queueKeyPrefix = "queue:"

func init() {
	resque.Register("redisv6", &drv{})
}

type drv struct {
	client *redis.Client
	driver.Enqueuer
	schedule  map[string]struct{}
	nameSpace string
}

func (d *drv) SetClient(name string, client interface{}) {
	d.client = client.(*redis.Client)
	d.schedule = make(map[string]struct{})
	d.nameSpace = name
}

func (d *drv) ListPush(queue string, jobJSON string) (int64, error) {
	return d.client.RPush(d.nameSpace+queueKeyPrefix+queue, jobJSON).Result()
}

func (d *drv) ListPushDelay(t time.Time, queue string, jobJSON string) (bool, error) {
	err := d.client.ZAdd(queue, redis.Z{
		Member: jobJSON,
		Score:  timeToSecondsWithNanoPrecision(t),
	}).Err()
	if err != nil {
		return false, err
	}
	if _, ok := d.schedule[queue]; !ok {
		d.schedule[queue] = struct{}{}
	}
	return true, nil
}
func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / 1000000000.0 // nanoSecondPrecision
}

func (d *drv) Poll() {
	go func(d *drv) {
		for {
			for key := range d.schedule {
				now := timeToSecondsWithNanoPrecision(time.Now())
				r, _ := d.client.ZRangeByScore(key, redis.ZRangeBy{
					Min: "-inf",
					Max: strconv.FormatFloat(now, 'E', -1, 64),
				}).Result()
				var jobs []string
				for _, job := range r {
					jobs = append(jobs, string(job))
				}
				if len(jobs) == 0 {
					continue
				}
				if removed, _ := d.client.ZRem(key, jobs[0]).Result(); removed > 0 {
					queue := strings.TrimPrefix(key, d.nameSpace)
					d.client.LPush(d.nameSpace+queueKeyPrefix+queue, jobs[0])
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}(d)
}

