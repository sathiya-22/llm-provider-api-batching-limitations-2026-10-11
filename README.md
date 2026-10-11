## LLM Batching Proxy

### The Problem
The current state of LLM provider SDKs, particularly for OpenAI, often presents limitations in how they leverage underlying API batching capabilities. While methods like `ChatModels.batch` or `Runnables.batch` exist, they frequently implement client-side parallelization rather than truly utilizing the provider's native batch endpoints (e.g., OpenAI's `/v1/batches`). This results in inefficient use of network resources, higher latency, and increased operational costs for developers who need to process multiple LLM requests efficiently. This is especially critical for agentic AI systems where multiple sub-agents or tools might concurrently require LLM inferences.

### Why this Project Shape/Stack
This project is implemented as a lightweight Go service. Go was chosen for its excellent concurrency primitives, low overhead, and strong networking capabilities, making it ideal for building a high-performance proxy. A service-based approach allows it to act as an intermediary, intercepting LLM requests, aggregating them, and then dispatching them to the actual LLM provider's *native batch API* when available, or performing intelligent client-side batching otherwise. This design keeps the complexity of batch management separate from the agent's core logic and can be easily deployed as a sidecar or a central service.

### Setup and Usage (Zero API Keys)

This prototype includes a mock OpenAI server to demonstrate the batching logic without requiring any actual API keys.

1.  **Build the Go service:**
    ```bash
    go build -o llm-batching-proxy main.go
    ```

2.  **Run the mock OpenAI server (in a separate terminal):**
    ```bash
    go run mock_openai_server.go
    ```
    This server will listen on `localhost:8081` and simulate OpenAI's chat and batch endpoints.

3.  **Run the LLM Batching Proxy:**
    ```bash
    ./llm-batching-proxy
    ```
    The proxy will listen on `localhost:8080`.

4.  **Send requests to the proxy using `curl`:**

    **Single Request (will be batched by the proxy if threshold met):**
    ```bash
    curl -X POST -H "Content-Type: application/json" \
         -d '{
               "model": "gpt-3.5-turbo",
               "messages": [{"role": "user", "content": "Hello, world!"}],
               "stream": false
             }' \
         http://localhost:8080/v1/chat/completions
    ```

    **Multiple Requests (demonstrating the proxy's ability to handle concurrent requests and potentially batch them):**
    Open several terminals and run the `curl` command above simultaneously, or use a script to send multiple requests in quick succession. Observe the logs from the `mock_openai_server.go` to see if requests are being received via the `/v1/batches` endpoint. The proxy's internal batching logic will collect requests up to `BATCH_SIZE` or `BATCH_WINDOW_MS` before forwarding them.

### Optional Real OpenAI Adapter
This prototype is designed to work with a real OpenAI API key if you configure it.

1.  **Set your OpenAI API Key:**
    ```bash
    export OPENAI_API_KEY="sk-YOUR_OPENAI_API_KEY"
    ```

2.  **Modify `main.go`:**
    Uncomment the `targetURL = "https://api.openai.com"` line and comment out `targetURL = "http://localhost:8081"`.
    ```go
    // targetURL := "https://api.openai.com" // Uncomment for real OpenAI
    targetURL := "http://localhost:8081"   // Comment out for real OpenAI
    ```

3.  **Rebuild and run:**
    ```bash
    go build -o llm-batching-proxy main.go
    ./llm-batching-proxy
    ```
    Requests will now be forwarded to the real OpenAI API, leveraging its native batching if the conditions (enough requests within the window) are met.

---
