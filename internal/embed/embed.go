// Package embed turns text into an embedding vector by calling Ollama.
//
// An "embedding" is a list of numbers describing what a piece of text MEANS.
// Texts with similar meaning get similar numbers, even with no words in common —
// which is what makes search-by-meaning possible. all-minilm produces 384 of them.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client talks to Ollama's HTTP API at baseURL, using the given model.
type Client struct {
	baseURL string
	model   string
	http    *http.Client
}

// New builds a client, e.g. New("http://localhost:11434", "all-minilm").
func New(baseURL, model string) *Client {
	return &Client{
		baseURL: baseURL,
		model:   model,
		http:    &http.Client{Timeout: 30 * time.Second}, // don't hang forever
	}
}

// these mirror Ollama's /api/embeddings request and response JSON shapes.
type embeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}
type embeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

// Embed converts text into its 384-number embedding via Ollama.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	// 1. Build the JSON request body: {"model":"all-minilm","prompt":"<text>"}.
	body, err := json.Marshal(embeddingRequest{Model: c.model, Prompt: text})
	if err != nil {
		return nil, err
	}

	// 2. Build a POST request to Ollama, carrying the caller's context (so it
	//    cancels if the request is cancelled / times out).
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// 3. Send it.
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed: ollama returned status %d", resp.StatusCode)
	}

	// 4. Decode the response and hand back the 384 floats.
	var er embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, fmt.Errorf("embed decode: %w", err)
	}
	if len(er.Embedding) == 0 {
		return nil, fmt.Errorf("embed: ollama returned an empty embedding")
	}
	return er.Embedding, nil
}
