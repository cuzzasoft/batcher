package microbatcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockBatchProcessor struct {
	ProcessBatchFunc func(jobs []Job) []JobResult
}

func (mbp *MockBatchProcessor) ProcessBatch(jobs []Job) []JobResult {
	return mbp.ProcessBatchFunc(jobs)
}

// cause we gotta keep things interesting om nomnomnomnom
var resultMap = map[string]string{
	"🥔": "🍟",
	"🍎": "🧃",
	"🌾": "🍞",
}

// If extra logging is desired, uncomment this function
//func TestMain(m *testing.M) {
//	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
//		Level: slog.LevelDebug,
//	})))
//	os.Exit(m.Run())
//}

// TestMicroBatcher tests the MicroBatcher functionality.
func TestMicroBatcher(t *testing.T) {
	tests := []struct {
		name            string
		batchSize       int
		batchInterval   time.Duration
		jobs            []Job
		expectedResults []JobResult
	}{
		{
			name:            "Empty batch test",
			batchSize:       10,
			batchInterval:   250 * time.Millisecond,
			jobs:            []Job{},
			expectedResults: []JobResult{},
		},
		{
			name:          "Single job with larger batch size test",
			batchSize:     10,
			batchInterval: 250 * time.Millisecond,
			jobs: []Job{
				{ID: "1", Data: "🥔"},
			},
			expectedResults: []JobResult{
				{Job: Job{ID: "1", Data: "🥔"}, Result: "🍟"},
			},
		},
		{
			name:          "Batch size same as jobs test",
			batchSize:     2,
			batchInterval: 250 * time.Millisecond,
			jobs: []Job{
				{ID: "1", Data: "🥔"},
				{ID: "2", Data: "🍎"},
			},
			expectedResults: []JobResult{
				{Job: Job{ID: "1", Data: "🥔"}, Result: "🍟"},
				{Job: Job{ID: "2", Data: "🍎"}, Result: "🧃"},
			},
		},
		{
			name:          "Jobs smaller than batch size so interval executes test",
			batchSize:     10,
			batchInterval: 250 * time.Millisecond,
			jobs: []Job{
				{ID: "1", Data: "🥔"},
				{ID: "2", Data: "🍎"},
				{ID: "3", Data: "🌾"},
			},
			expectedResults: []JobResult{
				{Job: Job{ID: "1", Data: "🥔"}, Result: "🍟"},
				{Job: Job{ID: "2", Data: "🍎"}, Result: "🧃"},
				{Job: Job{ID: "3", Data: "🌾"}, Result: "🍞"},
			},
		},
		{
			name:          "Shutdown executes jobs that are still in the batch test",
			batchSize:     10,
			batchInterval: 250 * time.Millisecond,
			jobs: []Job{
				{ID: "1", Data: "🥔"},
				{ID: "2", Data: "🍎"},
			},
			expectedResults: []JobResult{
				{Job: Job{ID: "1", Data: "🥔"}, Result: "🍟"},
				{Job: Job{ID: "2", Data: "🍎"}, Result: "🧃"},
			},
		},
		{
			name:          "Batch size and interval combination test",
			batchSize:     1,
			batchInterval: 250 * time.Millisecond,
			jobs: []Job{
				{ID: "1", Data: "🥔"},
				{ID: "2", Data: "🍎"},
				{ID: "3", Data: "🌾"},
			},
			expectedResults: []JobResult{
				{Job: Job{ID: "1", Data: "🥔"}, Result: "🍟"},
				{Job: Job{ID: "2", Data: "🍎"}, Result: "🧃"},
				{Job: Job{ID: "3", Data: "🌾"}, Result: "🍞"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProcessor := &MockBatchProcessor{
				ProcessBatchFunc: func(jobs []Job) []JobResult {
					var results []JobResult
					for _, job := range jobs {
						results = append(results, JobResult{
							Job:    job,
							Result: resultMap[job.Data.(string)],
						})
					}
					return results
				},
			}

			mb := NewMicroBatcher(mockProcessor, WithBatchSize(tt.batchSize), WithBatchInterval(tt.batchInterval))
			ctx := context.Background()

			// Start the MicroBatcher
			go mb.Start(ctx)

			// Submit jobs
			for _, job := range tt.jobs {
				mb.Submit(ctx, job)
			}

			// Get the results
			resultsCh := make(chan JobResult, len(tt.jobs))
			go func() {
				for result := range mb.Results(ctx) {
					resultsCh <- result
				}
				close(resultsCh)
			}()

			// ⏱️ give it a moment to process the jobs
			time.Sleep(2 * tt.batchInterval)

			// Shutdown and assert all is well in the universe
			err := mb.Shutdown(ctx)
			require.NoError(t, err)

			// Collect results from the channel to assert against
			var results []JobResult
			for result := range resultsCh {
				results = append(results, result)
			}

			assert.ElementsMatch(t, tt.expectedResults, results)
		})
	}
}
