package aggregate

import (
	"math"
	"testing"
)

// TestAggregateSingleTimeseries validates that Aggregate returns the input data if no modifications are required.
func TestAggregateSingleTimeseries(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 1)
	ts[0] = []Metric{
		{0, 10.1},
		{10, 15.2},
		{20, 20.4},
	}
	agg := Aggregate(ts, 0, 20, interval)
	exp := []Metric{
		{0, 10.1},
		{10, 15.2},
		{20, 20.4},
	}
	if !equal(exp, agg) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, agg)
	}
}

// TestAggregateSingleTimeseriesWithDuplicateSamplesRemoved makes sure that duplicate samples are not double counted.
func TestAggregateSingleTimeseriesWithDuplicateSamplesRemoved(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 1)
	ts[0] = []Metric{
		{0, 10.1},
		{10, 15.2},
		{10, 15.2},
		{10, 15.2},
		{20, 20.4},
	}
	agg := Aggregate(ts, 0, 20, interval)
	exp := []Metric{
		{0, 10.1},
		{10, 15.2},
		{20, 20.4},
	}
	if !equal(exp, agg) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, agg)
	}
}

// TestDuplicateSamplesRemoved makes sure that duplicate samples are not double counted.
func TestAggregateTimeseriesWithDuplicateTimestamps(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 2)
	ts[0] = []Metric{
		{0, 10.1},
		{10, 15.2},
		{10, 15.2},
		{10, 15.2},
		{20, 20.4},
	}
	ts[0] = []Metric{
		{0, 10.1},
		{10, 15.2},
		{10, 15.2},
		{10, 15.2},
		{20, 20.4},
		{20, 20.4},
		{20, 20.4},
	}
	agg := Aggregate(ts, 0, 20, interval)
	exp := []Metric{
		{0, 10.1},
		{10, 15.2},
		{20, 20.4},
	}
	if !equal(exp, agg) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, agg)
	}
}

// TestApplyRangeParams validates that Aggregate only aggregates points that lie within the range provided.
func TestApplyRangeParams(t *testing.T) {
	interval := uint32(100)
	ts := make([][]Metric, 2)
	ts[0] = []Metric{
		{0, 10.1},
		{100, 15.2},
		{200, 20.4},
		{300, 18.3},
	}
	ts[1] = []Metric{
		{0, 10.1},
		{100, 10.2},
		{200, 30.9},
		{300, 78.3},
	}
	got := Aggregate(ts, 100, 200, interval)
	exp := []Metric{
		{100, 25.4},
		{200, 51.3},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

func TestApplyRangeWhenMissingSamples(t *testing.T) {
	interval := uint32(50)
	ts := make([][]Metric, 2)
	ts[0] = []Metric{
		{150, 30.2},
		{250, 70.4},
		{350, 18.3},
	}
	ts[1] = []Metric{
		{150, 10.2},
		{350, 12.3},
	}
	got := Aggregate(ts, 50, 450, interval)
	exp := []Metric{
		{50, math.NaN()},
		{100, math.NaN()},
		{150, 40.4},
		{200, math.NaN()},
		{250, 70.4},
		{300, math.NaN()},
		{350, 30.6},
		{400, math.NaN()},
		{450, math.NaN()},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

// TestApplyRangeNotDivisibleByInterval ensures that irregular ranges still work.
func TestApplyRangeNotDivisibleByInterval(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 2)
	ts[0] = []Metric{
		{780, 50},
		{790, 50},
		{800, 150},
		{820, 150},
		{870, 42},
	}
	ts[1] = []Metric{
		{840, 68},
	}
	got := Aggregate(ts, 784, 872, interval)
	exp := []Metric{
		{790, 50},
		{800, 150},
		{810, math.NaN()},
		{820, 150},
		{830, math.NaN()},
		{840, 68},
		{850, math.NaN()},
		{860, math.NaN()},
		{870, 42},
		{880, math.NaN()},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

// TestApplyRangeNotDivisibleByIntervalWithNonDivisibleTimestamps ensures that irregular ranges still work and that non divisible timestamps are repaired to be divisible and regular.
func TestApplyRangeNotDivisibleByIntervalWithNonDivisibleTimestamps(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 2)
	ts[0] = []Metric{
		{781, 50},
		{792, 50},
		{800, 150},
		{825, 150},
		{877, 42},
	}
	ts[1] = []Metric{
		{843, 68},
	}
	got := Aggregate(ts, 784, 872, interval)
	exp := []Metric{
		{790, 50},
		{800, 50},
		{810, math.NaN()},
		{820, math.NaN()},
		{830, 150},
		{840, math.NaN()},
		{850, 68},
		{860, math.NaN()},
		{870, math.NaN()},
		{880, 42},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

// TestAggregateManyTimeseries ensures that a multitude of timeseries are aggregated properly.
func TestAggregateManyTimeseries(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 4)
	ts[0] = []Metric{
		{780, 10},
		{790, 10},
		{800, 150},
		{820, 150},
		{870, 42},
	}
	ts[1] = []Metric{
		{820, math.NaN()},
		{840, 100},
	}
	ts[2] = []Metric{
		{800, 5},
		{840, 10},
	}
	ts[3] = []Metric{
		{840, 15},
		{850, math.NaN()},
		{860, 16},
		{870, 17},
	}
	got := Aggregate(ts, 780, 870, interval)
	exp := []Metric{
		{780, 10},
		{790, 10},
		{800, 155},
		{810, math.NaN()},
		{820, 150},
		{830, math.NaN()},
		{840, 125},
		{850, math.NaN()},
		{860, 16},
		{870, 59},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

// TestEverything tests for all of the test conditions in one test.
func TestEverything(t *testing.T) {
	interval := uint32(10)
	ts := make([][]Metric, 4)
	ts[0] = []Metric{
		{781, 50},
		{792, 50},
		{800, 150},
		{825, 150},
		{877, 42},
	}
	ts[1] = []Metric{
		{825, math.NaN()},
		{843, 68},
	}
	ts[2] = []Metric{
		{843, 10},
	}
	ts[3] = []Metric{
		{843, 15},
		{843, math.NaN()},
		{843, 16},
		{843, 17},
	}
	got := Aggregate(ts, 784, 872, interval)
	exp := []Metric{
		{790, 50},
		{800, 50},
		{810, math.NaN()},
		{820, math.NaN()},
		{830, 150},
		{840, math.NaN()},
		{850, 93},
		{860, math.NaN()},
		{870, math.NaN()},
		{880, 42},
	}

	if !equal(exp, got) {
		t.Fatalf("output mismatch:\nexpected:\n%v\ngot:\n%v", exp, got)
	}
}

func equal(exp, got []Metric) bool {
	if len(exp) != len(got) {
		return false
	}
	for i, pgot := range got {
		pexp := exp[i]
		if math.IsNaN(pgot.Value) != math.IsNaN(pexp.Value) {
			return false
		}
		if !math.IsNaN(pgot.Value) && pgot.Value != pexp.Value {
			return false
		}
		if pgot.Ts != pexp.Ts {
			return false
		}
	}
	return true
}
