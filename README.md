# MicroBatcher

MicroBatcher is a Go package for managing and processing batches of jobs. It allows you to submit jobs to a queue, batch them together, and process them using a configurable batch processor. This package is useful for scenarios where you need to handle jobs in groups, providing both batching by size and time intervals.

## Features

- **Batching**: Collect jobs into batches based on size or time interval.
- **Concurrent Processing**: Process batches of jobs concurrently using a `BatchProcessor`.
- **Graceful Shutdown**: Ensure all jobs are processed before shutting down.
- **Configurable Options**: Customize batch size, interval, and the batch implementation.

## Installation

To include the `microbatcher` package in your Go project, use:

```sh
go get github.com/cuzzasoft/batcher
```

## Usage

### Start a new batcher

Provide your own `BatchProcessor` implementation and configure the batcher with the desired options.

```go	
batcher := microbatcher.NewMicroBatcher(
    &MyProcessor{},
    microbatcher.WithBatchSize(5),
    microbatcher.WithBatchInterval(2*time.Second),
)
batcher.Start(ctx)
```

### Listen for results

When your jobs are complete they will be sent back to  the results channel. This is obtained using the `Results` method.

```go
jobResultsCh := batcher.Results(ctx)
```

### Submit jobs

Submit jobs to the batcher using the `Submit` method. The batcher will batch the jobs together and process them according to the configured options.

```go
for i := 0; i < 25; i++ {
    batcher.Submit(ctx, microbatcher.Job{
        ID:   fmt.Sprintf("job-%d", i),
        Data: fmt.Sprintf("data-%d", i),
    })
}
```

### Graceful Shutdown

Shutdown the batcher to ensure all jobs are processed before exiting. This will wait for all jobs to be processed before returning.

```go
err := batcher.Shutdown(ctx)
if err != nil {
    return
}

