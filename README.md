# Insights Coding Exercise / Interview

A timeseries is a series of samples in ascending time order,
where each sample is a combination of a floating point numeric value 
and unix timestamp.

For example:
```
timeseries = (unix_timestamp, floating_point_value)
s1 = [(t1, 10), (t2, 30), (t3, 15)]
```

The process of combining multiple timeseries can be referred to as
aggregation.  This allows us to distill multiple related timeseries
into a single timeseries that is descriptive or representative of all
timeseries that were used in the aggregation.

For the purpose of this exercise our aggregation will use point wise summing
of sample values at the same timestamp.

For example:
```
s1 = [(t1, 10), (t2, 30), (t3, 15)]
s2 = [(t1, 15), (t2, 20), (t3, 15)]

agg = [(t1, 25), (t2, 50), (t3, 30)]
```

Raw timeseries data is problematic as it can have missing samples, duplicate samples,
and irregular sample spacing. These issues need to be remedied through normalization 
of the raw samples so that the individual timeseries can be cleanly aggregated together.

We need a function that converts multiple, related, raw timeseries into a single clean timeseries that
represents the aggregated values at each point in time requested. The `from` and `to` parameters
determine the range for which the aggregated series should span.  Points that fall outside of this range should not
be included in the resultant timeseries.

In order to complete the exercise fill in the `Aggregate` function and ensure that all unit tests pass.

The input data is represented as a [][]Metric.  You can also think of this is a slice of timeseries in 
which each timeseries contains a slice of metric samples. The output timeseries is a slice of metrics 
where each sample is the aggregated sample from the input series at that point in time. The `interval` is the desired 
number of seconds between successive samples. The `from` and `to` parameters are represented as the number of
seconds since the unix epoch.

Each timeseries ([]Metric) in the input data has the following properties:
1) It may include points that are outside the time range we want to aggregate.
2) The spacing between samples may or may not be consistent.
3) There might or duplicate or missing samples.
4) The samples are monotonically increasing in timestamp on a per timeseries basis.

The aggregated resultant timeseries must have these properties:
1) Missing samples should use the value `math.NaN()`.
2) Samples must be equally spaced on the provided interval.
3) Samples must have timestamps that are multiples of the interval.
   For example for interval=100, valid timestamps are 100, 200, 300, etc.
   If the input timestamp is not a multiple of the interval, it should be adjusted to the next higher one that is.
4) If multiple samples in a single timeseries correspond to the same timestamp (duplicates), the first sample should be 
   the one used for the aggregation and the other sample can be discarded.
5) Samples must have a timestamp >= `from` and <= `to` (adjusted to be a multiple of the interval as in #3).
