```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	PROXY_PORT       = ":8080"
	BATCH_SIZE       = 5    // Max requests per batch
	BATCH_WINDOW_MS  = 1000 // Time window in milliseconds to collect requests
	MAX_QUEUE_SIZE   = 100  // Max requests in the internal queue
	BATCH_ENDPOINT   = "/v1/batches"
	CHAT_COMP_PATH   = "/v1/chat/completions"
	BATCH_FILE_PATH  = "/tmp/batch_input.jsonl" // Temporary file for batch input
	BATCH_POLL_INTERVAL = 5 * time.Second // How often to poll for batch status
)

// OpenAIRequest represents a minimal structure for a chat completion request
type OpenAIRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

// Message represents a message in a chat completion request
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// InternalRequest encapsulates an incoming HTTP request for batching
type InternalRequest struct {
	ID        string
	Request   OpenAIRequest
	Timestamp time.Time
	ResponseC chan *http.Response
	ErrorC    chan error
}

// OpenAIBatchInputEntry for the /v1/batches input file
type OpenAIBatchInputEntry struct {
	CustomID string          `json:"custom_id"`
	Method   string          `json:"method"`
	URL      string          `json:"url"`
	Body     OpenAIRequest   `json:"body"`
}

// OpenAIBatchRequest for the /v1/batches API
type OpenAIBatchRequest struct {
	InputFileID  string `json:"input_file_id"`
	Endpoint     string `json:"endpoint"`
	CompletionWindow string `json:"completion_window"` // e.g., "24h"
}

// OpenAIFile represents the response from /v1/files
type OpenAIFile struct {
	ID string `json:"id"`
	Object string `json:"object"`
	Bytes int `json:"bytes"`
	CreatedAt int `json:"created_at"`
	Filename string `json:"filename"`
	Purpose string `json:"purpose"`
	Status string `json:"status"`
}

// OpenAIBatch represents the response from /v1/batches
type OpenAIBatch struct {
	ID string `json:"id"`
	Object string `json:"object"`
	Endpoint string `json:"endpoint"`
	Errors interface{} `json:"errors"` // Can be null or an object
	FileID string `json:"input_file_id"`
	OutputFileID *string `json:"output_file_id"` // Can be null
	CreatedAt int `json:"created_at"`
	ExpiresAt int `json:"expires_at"`
	CompletedAt *int `json:"completed_at"` // Can be null
	Status string `json:"status"`
	RequestCounts map[string]int `json:"request_counts"`
	Metadata interface{} `json:"metadata"`
}

// A simple map to hold pending requests, keyed by custom_id
var pendingRequests sync.Map // map[string]*InternalRequest

var (
	requestQueue = make(chan *InternalRequest, MAX_QUEUE_SIZE)
	batchMux     sync.Mutex
	httpClient   = &http.Client{Timeout: 30 * time.Second} // Increased timeout for batch
	targetURL    = "http://localhost:8081" // Default to mock server
)

func init() {
	if os.Getenv("OPENAI_API_KEY") != "" {
		targetURL = "https://api.openai.com" // Uncomment for real OpenAI
		log.Printf("Using real OpenAI API at %s", targetURL)
	} else {
		log.Printf("OPENAI_API_KEY not set, using mock OpenAI API at %s", targetURL)
	}
}

func main() {
	go batchProcessor()

	http.HandleFunc(CHAT_COMP_PATH, handleChatCompletions)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("LLM Batching Proxy listening on %s", PROXY_PORT)
	log.Fatal(http.ListenAndServe(PROXY_PORT, nil))
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	var req OpenAIRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	internalReq := &InternalRequest{
		ID:        uuid.New().String(),
		Request:   req,
		Timestamp: time.Now(),
		ResponseC: make(chan *http.Response, 1),
		ErrorC:    make(chan error, 1),
	}

	select {
	case requestQueue <- internalReq:
		// Request queued, now wait for response
		select {
		case resp := <-internalReq.ResponseC:
			defer resp.Body.Close()
			for k, v := range resp.Header {
				for _, vv := range v {
					w.Header().Add(k, vv)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
		case err := <-internalReq.ErrorC:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		case <-r.Context().Done(): // Client disconnected
			log.Printf("Client disconnected for request %s", internalReq.ID)
			// Remove from pendingRequests if still there
			pendingRequests.Delete(internalReq.ID)
		}
	default:
		http.Error(w, "Service overloaded, please try again later", http.StatusServiceUnavailable)
	}
}

func batchProcessor() {
	var currentBatch []*InternalRequest
	ticker := time.NewTicker(BATCH_WINDOW_MS * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-requestQueue:
			batchMux.Lock()
			currentBatch = append(currentBatch, req)
			pendingRequests.Store(req.ID, req) // Store for later lookup
			log.Printf("Request %s queued. Current batch size: %d", req.ID, len(currentBatch))
			if len(currentBatch) >= BATCH_SIZE {
				log.Println("Batch size limit reached, processing batch.")
				go processBatch(currentBatch)
				currentBatch = nil // Clear the batch
				ticker.Reset(BATCH_WINDOW_MS * time.Millisecond) // Reset ticker
			}
			batchMux.Unlock()

		case <-ticker.C:
			batchMux.Lock()
			if len(currentBatch) > 0 {
				log.Printf("Batch window expired with %d requests, processing batch.", len(currentBatch))
				go processBatch(currentBatch)
				currentBatch = nil // Clear the batch
			}
			batchMux.Unlock()
		}
	}
}

func processBatch(batch []*InternalRequest) {
	if len(batch) == 0 {
		return
	}
	log.Printf("Processing batch of %d requests...", len(batch))

	// Step 1: Create a JSONL file for batch input
	fileContent := &bytes.Buffer{}
	for _, req := range batch {
		batchEntry := OpenAIBatchInputEntry{
			CustomID: req.ID,
			Method:   "POST",
			URL:      CHAT_COMP_PATH,
			Body:     req.Request,
		}
		jsonLine, err := json.Marshal(batchEntry)
		if err != nil {
			log.Printf("Error marshaling batch entry for %s: %v", req.ID, err)
			req.ErrorC <- fmt.Errorf("failed to prepare batch request: %w", err)
			continue
		}
		fileContent.Write(jsonLine)
		fileContent.WriteString("\n")
	}

	// Step 2: Upload the file to OpenAI
	uploadReq, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/files", targetURL), fileContent)
	if err != nil {
		handleBatchError(batch, fmt.Errorf("failed to create upload request: %w", err))
		return
	}
	uploadReq.Header.Set("Authorization", "Bearer "+os.Getenv("OPENAI_API_KEY"))
	uploadReq.Header.Set("Content-Type", "application/jsonl") // OpenAI expects application/jsonl for files
	uploadReq.Header.Set("x-openai-purpose", "batch") // Required for batch input files

	uploadResp, err := httpClient.Do(uploadReq)
	if err != nil {
		handleBatchError(batch, fmt.Errorf("failed to upload batch file: %w", err))
		return
	}
	defer uploadResp.Body.Close()

	if uploadResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(uploadResp.Body)
		handleBatchError(batch, fmt.Errorf("failed to upload batch file, status: %d, body: %s", uploadResp.StatusCode, respBody))
		return
	}

	var fileResponse OpenAIFile
	if err := json.NewDecoder(uploadResp.Body).Decode(&fileResponse); err != nil {
		handleBatchError(batch, fmt.Errorf("failed to decode file upload response: %w", err))
		return
	}
	log.Printf("Uploaded batch input file: %s (ID: %s)", fileResponse.Filename, fileResponse.ID)

	// Step 3: Create the batch job
	batchJobReqBody, _ := json.Marshal(OpenAIBatchRequest{
		InputFileID: fileResponse.ID,
		Endpoint:    CHAT_COMP_PATH,
		CompletionWindow: "24h", // Example completion window
	})
	createBatchReq, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/batches", targetURL), bytes.NewReader(batchJobReqBody))
	if err != nil {
		handleBatchError(batch, fmt.Errorf("failed to create batch job request: %w", err))
		return
	}
	createBatchReq.Header.Set("Authorization", "Bearer "+os.Getenv("OPENAI_API_KEY"))
	createBatchReq.Header.Set("Content-Type", "application/json")

	createBatchResp, err := httpClient.Do(createBatchReq)
	if err != nil {
		handleBatchError(batch, fmt.Errorf("failed to create batch job: %w", err))
		return
	}
	defer createBatchResp.Body.Close()

	if createBatchResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(createBatchResp.Body)
		handleBatchError(batch, fmt.Errorf("failed to create batch job, status: %d, body: %s", createBatchResp.StatusCode, respBody))
		return
	}

	var batchJobResponse OpenAIBatch
	if err := json.NewDecoder(createBatchResp.Body).Decode(&batchJobResponse); err != nil {
		handleBatchError(batch, fmt.Errorf("failed to decode batch job response: %w", err))
		return
	}
	log.Printf("Batch job created: %s (ID: %s, Status: %s)", batchJobResponse.Object, batchJobResponse.ID, batchJobResponse.Status)

	// Step 4: Poll for batch job completion
	go pollBatchJob(batchJobResponse.ID, batch)
}

func pollBatchJob(batchID string, originalRequests []*InternalRequest) {
	ticker := time.NewTicker(BATCH_POLL_INTERVAL)
	defer ticker.Stop()

	for range ticker.C {
		getBatchReq, err := http.NewRequest("GET", fmt.Sprintf("%s/v1/batches/%s", targetURL, batchID), nil)
		if err != nil {
			log.Printf("Error creating get batch request for %s: %v", batchID, err)
			handleBatchError(originalRequests, err)
			return
		}
		getBatchReq.Header.Set("Authorization", "Bearer "+os.Getenv("OPENAI_API_KEY"))

		getBatchResp, err := httpClient.Do(getBatchReq)
		if err != nil {
			log.Printf("Error getting batch status for %s: %v", batchID, err)
			// Continue polling, might be a transient network error
			continue
		}
		defer getBatchResp.Body.Close()

		if getBatchResp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(getBatchResp.Body)
			log.Printf("Failed to get batch status for %s, status: %d, body: %s", batchID, getBatchResp.StatusCode, respBody)
			handleBatchError(originalRequests, fmt.Errorf("failed to get batch status: %s", respBody))
			return
		}

		var batchStatus OpenAIBatch
		if err := json.NewDecoder(getBatchResp.Body).Decode(&batchStatus); err != nil {
			log.Printf("Error decoding batch status for %s: %v", batchID, err)
			handleBatchError(originalRequests, fmt.Errorf("failed to decode batch status: %w", err))
			return
		}

		log.Printf("Batch job %s status: %s", batchID, batchStatus.Status)

		if batchStatus.Status == "completed" && batchStatus.OutputFileID != nil {
			log.Printf("Batch job %s completed. Output file ID: %s", batchID, *batchStatus.OutputFileID)
			downloadBatchResults(*batchStatus.OutputFileID, originalRequests)
			return // Job completed, stop polling
		} else if batchStatus.Status == "failed" || batchStatus.Status == "cancelled" || batchStatus.Status == "expired" {
			log.Printf("Batch job %s failed or cancelled. Status: %s", batchID, batchStatus.Status)
			handleBatchError(originalRequests, fmt.Errorf("batch job %s %s", batchID, batchStatus.Status))
			return // Job failed, stop polling
		}
	}
}

func downloadBatchResults(outputFileID string, originalRequests []*InternalRequest) {
	downloadReq, err := http.NewRequest("GET", fmt.Sprintf("%s/v1/files/%s/content", targetURL, outputFileID), nil)
	if err != nil {
		handleBatchError(originalRequests, fmt.Errorf("failed to create download request for output file: %w", err))
		return
	}
	downloadReq.Header.Set("Authorization", "Bearer "+os.Getenv("OPENAI_API_KEY"))

	downloadResp, err := httpClient.Do(downloadReq)
	if err != nil {
		handleBatchError(originalRequests, fmt.Errorf("failed to download batch output file: %w", err))
		return
	}
	defer downloadResp.Body.Close()

	if downloadResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(downloadResp.Body)
		handleBatchError(originalRequests, fmt.Errorf("failed to download batch output file, status: %d, body: %s", downloadResp.StatusCode, respBody))
		return
	}

	// Read and process the JSONL output file
	scanner := json.NewDecoder(downloadResp.Body)
	var batchResults []map[string]interface{}
	for scanner.More() {
		var result map[string]interface{}
		if err := scanner.Decode(&result); err != nil {
			log.Printf("Error decoding batch result line: %v", err)
			continue
		}
		batchResults = append(batchResults, result)
	}

	// Distribute results back to original requests
	for _, result := range batchResults {
		customID, ok := result["custom_id"].(string)
		if !ok {
			log.Printf("Batch result missing custom_id: %+v", result)
			continue
		}

		if val, loaded := pendingRequests.Load(customID); loaded {
			originalReq := val.(*InternalRequest)
			response := result["response"].(map[string]interface{}) // Assuming response exists
			statusCode := int(response["status_code"].(float64)) // JSON numbers are float64

			// Reconstruct http.Response for the original client
			respBodyBytes, _ := json.Marshal(response["body"])
			resp := &http.Response{
				StatusCode: statusCode,
				Body:       io.NopCloser(bytes.NewReader(respBodyBytes)),
				Header:     make(http.Header),
			}
			// Add content type header
			resp.Header.Set("Content-Type", "application/json")

			select {
			case originalReq.ResponseC <- resp:
				// Response sent
			default:
				log.Printf("Client for request %s already disconnected, could not send response", customID)
			}
			pendingRequests.Delete(customID)
		} else {
			log.Printf("No pending request found for custom_id: %s", customID)
		}
	}

	// Handle any requests that were in the batch but didn't get a result
	for _, req := range originalRequests {
		if _, loaded := pendingRequests.Load(req.ID); loaded {
			log.Printf("Request %s did not receive a response from batch output.", req.ID)
			select {
			case req.ErrorC <- fmt.Errorf("no response received from batch job"):
			default:
				log.Printf("Client for request %s already disconnected, could not send error", req.ID)
			}
			pendingRequests.Delete(req.ID)
		}
	}
}

func handleBatchError(batch []*InternalRequest, err error) {
	log.Printf("Batch processing error: %v", err)
	for _, req := range batch {
		if _, loaded := pendingRequests.Load(req.ID); loaded {
			select {
			case req.ErrorC <- err:
			default:
				log.Printf("Client for request %s already disconnected, could not send error", req.ID)
			}
			pendingRequests.Delete(req.ID)
		}
	}
}
```
