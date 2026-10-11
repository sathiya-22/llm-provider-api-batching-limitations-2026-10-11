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
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

const MOCK_SERVER_PORT = ":8081"

// Mock internal storage
var (
	mockFiles  = make(map[string]MockFile)
	mockBatches = make(map[string]MockBatch)
	mu         sync.Mutex
)

type MockFile struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int    `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
	Status    string `json:"status"`
	Content   []byte `json:"-"` // Store content for later retrieval
}

type MockBatch struct {
	ID             string                 `json:"id"`
	Object         string                 `json:"object"`
	Endpoint       string                 `json:"endpoint"`
	Errors         interface{}            `json:"errors"`
	InputFileID    string                 `json:"input_file_id"`
	OutputFileID   *string                `json:"output_file_id"`
	CreatedAt      int64                  `json:"created_at"`
	ExpiresAt      int64                  `json:"expires_at"`
	CompletedAt    *int64                 `json:"completed_at"`
	Status         string                 `json:"status"` // "validating", "pending", "in_progress", "completed", "failed", "cancelled", "expired"
	RequestCounts  map[string]int         `json:"request_counts"`
	Metadata       interface{}            `json:"metadata"`
	CompletionWindow string             `json:"completion_window"`
}

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

// OpenAIBatchInputEntry for the /v1/batches input file
type OpenAIBatchInputEntry struct {
	CustomID string          `json:"custom_id"`
	Method   string          `json:"method"`
	URL      string          `json:"url"`
	Body     OpenAIRequest   `json:"body"`
}

func main() {
	http.HandleFunc("/v1/chat/completions", handleMockChatCompletions)
	http.HandleFunc("/v1/files", handleMockFiles)
	http.HandleFunc("/v1/files/", handleMockFiles) // For /v1/files/{file_id}/content
	http.HandleFunc("/v1/batches", handleMockBatches)
	http.HandleFunc("/v1/batches/", handleMockBatches) // For /v1/batches/{batch_id}

	log.Printf("Mock OpenAI Server listening on %s", MOCK_SERVER_PORT)
	log.Fatal(http.ListenAndServe(MOCK_SERVER_PORT, nil))
}

func handleMockChatCompletions(w http.ResponseWriter, r *http.Request) {
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

	log.Printf("Mock Chat Completions received request for model: %s", req.Model)

	// Simulate a simple response
	resp := map[string]interface{}{
		"id":      fmt.Sprintf("chatcmpl-%s", uuid.New().String()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   req.Model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": fmt.Sprintf("Mock response for: \"%s\"", req.Messages[0].Content),
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]int{
			"prompt_tokens":     len(req.Messages[0].Content) / 4, // Estimate
			"completion_tokens": 10,
			"total_tokens":      (len(req.Messages[0].Content) / 4) + 10,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleMockFiles(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	switch r.Method {
	case http.MethodPost: // Upload file
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading file content", http.StatusInternalServerError)
			return
		}

		purpose := r.Header.Get("x-openai-purpose")
		if purpose == "" {
			http.Error(w, "Missing x-openai-purpose header", http.StatusBadRequest)
			return
		}

		fileID := fmt.Sprintf("file-%s", uuid.New().String())
		filename := fmt.Sprintf("batch_input_%s.jsonl", uuid.New().String()[:8])

		mockFile := MockFile{
			ID:        fileID,
			Object:    "file",
			Bytes:     len(fileContent),
			CreatedAt: time.Now().Unix(),
			Filename:  filename,
			Purpose:   purpose,
			Status:    "uploaded",
			Content:   fileContent,
		}
		mockFiles[fileID] = mockFile

		log.Printf("Mock File uploaded: %s (ID: %s, Purpose: %s)", mockFile.Filename, mockFile.ID, mockFile.Purpose)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockFile)

	case http.MethodGet: // Get file content
		// Expects /v1
