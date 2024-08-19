package main

import (
	"context"
	"fmt"
	microbatcher "github.com/cuzzasoft/batcher"
	"log/slog"
	"os"
	"time"
)

type MyProcessor struct{}

// ProcessBatch should be replaced with your own logic to process the jobs.
func (p *MyProcessor) ProcessBatch(jobs []microbatcher.Job) []microbatcher.JobResult {
	var results []microbatcher.JobResult
	for _, job := range jobs {
		results = append(results, microbatcher.JobResult{
			Job:    job,
			Result: job.Data,
		})
	}

	return results
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	ctx := context.Background()
	batcher := microbatcher.NewMicroBatcher(
		&MyProcessor{},
		microbatcher.WithBatchSize(5),
		microbatcher.WithBatchInterval(2*time.Second),
	)
	batcher.Start(ctx)

	// Listen for results
	jobResultsCh := batcher.Results(ctx)

	// Submit jobs
	for i := 0; i < 25; i++ {
		batcher.Submit(ctx, microbatcher.Job{
			ID:   fmt.Sprintf("job-%d", i),
			Data: fmt.Sprintf("data-%d", i),
		})
	}

	// Retrieve results
	go func() {
		for {
			result := <-jobResultsCh
			if result.Error != nil {
				fmt.Printf("Job ID: %s, Error: %v\n", result.Job.ID, result.Error)
			} else {
				fmt.Printf("Job ID: %s, Result: %v\n", result.Job.ID, result.Result)
			}
		}
	}()

	// Give it a moment to process the jobs
	time.Sleep(4 * time.Second)

	err := batcher.Shutdown(ctx)
	if err != nil {
		return
	}
}
