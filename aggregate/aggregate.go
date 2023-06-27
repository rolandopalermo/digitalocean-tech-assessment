package aggregate

import (
	"math"
	"sort"
)

type Metric struct {
	Ts    uint32
	Value float64
}

func Aggregate(metrics [][]Metric, start, end, interval uint32) []Metric {
	result := make([]Metric, 0)

	for _, metric := range metrics {
		result = Merge(
			FilterByTsRange(result, start, end, interval),
			FilterByTsRange(metric, start, end, interval),
		)
	}

	return result
}

func Merge(metrics1 []Metric, metrics2 []Metric) []Metric {
	metrics1 = append(metrics1, metrics2...)
	bucket := make(map[uint32]float64)

	for _, metric := range metrics1 {
		existingValue, ok := bucket[metric.Ts]
		if !ok {
			bucket[metric.Ts] = metric.Value
		} else {
			bucket[metric.Ts] = SumNaNs(existingValue, metric.Value)
		}
	}

	result := make([]Metric, 0, len(bucket))

	keys := make([]uint32, 0, len(bucket))
	for k := range bucket {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		result = append(result, Metric{
			Ts:    k,
			Value: bucket[k],
		})
	}

	return result
}

func FilterByTsRange(metrics []Metric, start, end, interval uint32) []Metric {
	var result []Metric

	bucket := ToMap(RemoveDuplicates(metrics), interval)

	start = FindClosestMultiple(start, interval)

	end = FindClosestMultiple(end, interval)

	for i := start; i <= end; i = i + interval {
		if value, ok := bucket[i]; ok {
			result = append(result, Metric{
				Ts:    i,
				Value: value,
			})
		} else {
			result = append(result, Metric{
				Ts:    i,
				Value: math.NaN(),
			})
		}
	}

	return result
}

func RemoveDuplicates(metrics []Metric) []Metric {
	bucket := make(map[Metric]bool)

	var result []Metric

	for _, metric := range metrics {
		if _, ok := bucket[metric]; !ok {
			bucket[metric] = true
			result = append(result, metric)
		}
	}

	return result
}

func ToMap(metrics []Metric, interval uint32) map[uint32]float64 {
	bucket := make(map[uint32]float64)

	for _, metric := range metrics {
		ts := metric.Ts
		if ts%interval != 0 {
			ts = FindClosestMultiple(ts, interval)
		}

		_, ok := bucket[ts]

		if !ok {
			bucket[ts] = metric.Value
		}

	}

	return bucket
}

func SumNaNs(variable1, variable2 float64) float64 {
	if math.IsNaN(variable1) {
		return variable2
	}
	if math.IsNaN(variable2) {
		return variable1
	}
	return variable1 + variable2
}

func FindClosestMultiple(n, m uint32) uint32 {
	if n%m == 0 {
		return n
	}

	quotient := n / m
	upperMultiple := m * (quotient + 1)
	return upperMultiple
}
