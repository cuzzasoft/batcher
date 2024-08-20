package microbatcher

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Job represents a single unit of work provided to the BatchProcessor.
type Job struct {
	ID   string
	Data interface{}
}

// JobResult represents the result of a processed job provided by the BatchProcessor.
type JobResult struct {
	Job    Job
	Error  error
	Result interface{}
}

// Batch represents a collection of jobs. Must be thread-safe.
type Batch interface {
	Add(job Job)
	Size() int
	Drain() []Job
}

type JobBatch struct {
	mu   sync.Mutex
	jobs []Job
}

// NewJobBatch creates a new JobBatch.
func NewJobBatch() *JobBatch {
	return &JobBatch{
		jobs: make([]Job, 0),
	}
}

// Add adds a Job to the batch.
func (b *JobBatch) Add(job Job) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.jobs = append(b.jobs, job)
}

// Size returns the number of Jobs currently in the batch.
func (b *JobBatch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.jobs)
}

// Drain clears and returns all jobs in the batch.
func (b *JobBatch) Drain() []Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	jobs := make([]Job, len(b.jobs))
	copy(jobs, b.jobs)

	// Clear the batch
	b.jobs = b.jobs[:0]

	return jobs
}

// BatchProcessor should be implemented to handle processing the batches of jobs. It is responsible for handling any errors / timeouts in the batch.
type BatchProcessor interface {
	ProcessBatch(jobs []Job) []JobResult
}

// Option for configuring the MicroBatcher.
type Option func(*MicroBatcher)

// WithBatchSize sets the batch size for the MicroBatcher. This is how many jobs are batched together before being processed.
func WithBatchSize(size int) Option {
	return func(mb *MicroBatcher) {
		mb.batchSize = size
		mb.todo = make(chan Job)
	}
}

// WithBatchInterval sets the batch interval for the MicroBatcher. This is how often the batch is processed, regardless of the batch size.
func WithBatchInterval(interval time.Duration) Option {
	return func(mb *MicroBatcher) {
		mb.batchInterval = interval
	}
}

// WithBatch sets the batch for the MicroBatcher. This is how the jobs are batched together before being processed.
func WithBatch(batch Batch) Option {
	return func(mb *MicroBatcher) {
		mb.batch = batch
	}
}

// MicroBatcher manages handling of the Job submission, batching the Job, and processing of it.
// When a Job is submitted, it is added to the batch and processed by the BatchProcessor if the batch size is reached.
// When the batchInterval is reached, the batch is processed regardless of the batch size.
// See Shutdown for details on shutdown.
type MicroBatcher struct {
	processor     BatchProcessor
	batchSize     int
	batchInterval time.Duration
	todo          chan Job
	done          chan JobResult
	shutdownC     chan struct{}
	goroutines    sync.WaitGroup
	batch         Batch
	timer         *time.Ticker
}

// NewMicroBatcher creates a new MicroBatcher with the provided BatchProcessor, batch size, and batch interval.
// The default batch size is 10, and the default batch interval is 3 minutes.
func NewMicroBatcher(processor BatchProcessor, options ...Option) *MicroBatcher {
	mb := &MicroBatcher{
		processor:     processor,
		batchSize:     10,
		batchInterval: 3 * time.Minute,
		done:          make(chan JobResult),
		shutdownC:     make(chan struct{}, 1),
		batch:         NewJobBatch(),
	}

	for _, opt := range options {
		opt(mb)
	}

	return mb
}

// Start initializes the processing loop for batching jobs. This will process the jobs in batches and send the results
// to the results channel. If shutdown is called, the processing loop will be stopped and the results channel will be shutdown.
func (mb *MicroBatcher) Start(context.Context) {
	slog.Info("Starting microbatcher")
	mb.goroutines.Add(1)

	go func() {
		defer mb.goroutines.Done()
		mb.timer = time.NewTicker(mb.batchInterval)
		defer mb.timer.Stop()

		for {
			select {
			case job := <-mb.todo:
				slog.Debug("Job received, adding to batch", "job", job)
				mb.addJob(job)
			case <-mb.timer.C:
				slog.Debug("Batch interval reached")
				mb.processJobs()
			case <-mb.shutdownC:
			DONE:
				for {
					select {
					case job := <-mb.todo:
						slog.Debug("Shutdown received, adding remaining job", "job", job)
						mb.addJob(job)
					default:
						break DONE
					}
				}
				slog.Debug("Shutdown received, processing remaining jobs")
				mb.processJobs()
				return
			}
		}
	}()
}

func (mb *MicroBatcher) resetTimer() {
	if mb.timer != nil {
		slog.Debug("Resetting timer")
		mb.timer.Reset(mb.batchInterval)
	}
}

// addJob adds a job to the batch and processes if the batch size is reached. Locks the MicroBatcher until the job is added.
func (mb *MicroBatcher) addJob(job Job) {
	slog.Debug("Adding job to batch", "job", job)

	mb.batch.Add(job)

	// Process the batch if the batch size is reached.
	if mb.batch.Size() >= mb.batchSize {
		mb.processJobs()
	}
}

// processJobs processes the current batch of jobs. Locks the MicroBatcher until the batch is allocated to process.
// If the batch is empty, it will do nothing. Resets the timer after scheduling for processing.
func (mb *MicroBatcher) processJobs() {
	if mb.batch.Size() == 0 {
		return
	}

	batch := mb.batch.Drain()

	mb.goroutines.Add(1)
	go mb.processBatch(batch)

	mb.resetTimer()
}

// processBatch processes a batch of jobs using the BatchProcessor and sends results to the results channel.
func (mb *MicroBatcher) processBatch(batch []Job) {
	defer mb.goroutines.Done()

	if len(batch) == 0 {
		slog.Debug("No jobs to send to ProcessBatch")
		return
	}

	slog.Debug("Processing batch", "batch", batch)
	// The processor is responsible for handling any errors / timeouts in the batch and returning a result with the appropriate Error if so.
	results := mb.processor.ProcessBatch(batch)

	// We should probably have a way to handle the case where the results channel is closed. This could be done with retry logic with backoff,
	// or a timeout, then simply dropping results when it gives up. It simply expects the results channel to be open until all results are sent.
	for _, result := range results {
		slog.Debug("Sending result to results channel", "result", result)
		mb.done <- result
	}
	slog.Debug("Done processing batch", "batch", batch)
}

// Submit a Job to the micro-batcher.
func (mb *MicroBatcher) Submit(ctx context.Context, job Job) {
	mb.todo <- job
}

// Results returns a channel to receive the results of processing as JobResult. This channel is closed when Shutdown is called.
func (mb *MicroBatcher) Results(context.Context) <-chan JobResult {
	return mb.done
}

// Shutdown stops the MicroBatcher and waits for all jobs to be processed.
// When calling Shutdown, the system processes the batch and sends the results to the results channel.
func (mb *MicroBatcher) Shutdown(context.Context) error {
	slog.Info("Shutting down microbatcher")
	close(mb.shutdownC)

	slog.Debug("Waiting for routines to close")
	mb.goroutines.Wait()

	slog.Debug("Closing results channel")
	close(mb.done)

	return nil
}
