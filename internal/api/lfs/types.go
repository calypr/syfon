package lfs

type Options struct {
	MaxBatchObjects              int
	MaxBatchBodyBytes            int64
	RequestLimitPerMinute        int
	BandwidthLimitBytesPerMinute int64
}

type windowCounter struct {
	Minute int64
	Count  int
}

type windowBytes struct {
	Minute int64
	Bytes  int64
}
