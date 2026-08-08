// Package llm talks to Ollama's chat API to GENERATE text.
//
// Deliberately separate from internal/embed even though both call Ollama, because
// they are very different workloads: embedding is ONE forward pass per message
// (fast, cheap); generation is one forward pass PER TOKEN produced (slow, and
// inherently sequential — token N+1 can't be computed before token N). Keeping
// them in separate packages means they can point at different hosts and different
// models independently, which matters when one box can do one but not the other.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client talks to Ollama's HTTP API at baseURL, using the given model.
type Client struct {
	baseURL string
	model   string
	http    *http.Client
}

// New builds a client, e.g. New("http://localhost:11434", "llama3.2:3b").
func New(baseURL, model string) *Client {
	return &Client{
		baseURL: baseURL,
		model:   model,
		// Far longer than the embedder's 30s: a few hundred tokens on modest
		// hardware genuinely takes minutes. Callers should still pass a context
		// deadline — this is only the last-resort ceiling.
		http: &http.Client{Timeout: 5 * time.Minute},
	}
}

// message is one turn in the conversation sent to Ollama.
type message struct {
	Role    string `json:"role"` // "system" | "user" | "assistant"
	Content string `json:"content"`
}

// options tunes generation. Temperature controls randomness: 0 is nearly
// deterministic (always take the most likely next token), 1+ is creative. For RAG
// we want it LOW — the bot's job is to report what the retrieved messages say,
// not to be imaginative. Invention is precisely the failure mode we're guarding
// against, so we deliberately turn the creativity down.
type options struct {
	Temperature float64 `json:"temperature"`
	NumPredict  int     `json:"num_predict"`
}

// maxOutputTokens caps how much the model may generate.
//
// Generation is strictly serial, so wall-clock time is essentially
// (output tokens ÷ tokens-per-second) — output length IS latency. On the homelab
// box that's ~4.66 tok/s, so this cap bounds a reply at roughly 25 seconds.
//
// It's a BACKSTOP, not a style control: the system prompt asks for two or three
// sentences and observed replies run 7-47 tokens, so this should never actually
// bind. It exists so a model that starts rambling can't stall the consumer.
const maxOutputTokens = 120

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
	Stream   bool      `json:"stream"` // false = one complete JSON reply, not a token stream
	Options  options   `json:"options"`
}

type chatResponse struct {
	Message message `json:"message"`
	// Done marks the final object in a streamed response. It arrives with empty
	// content and the timing statistics, so it means "that's all" rather than
	// carrying any text of its own.
	Done bool `json:"done"`
}

// Chat sends a system prompt (the bot's standing instructions) plus a user prompt
// (the question and its retrieved context) and returns the model's reply.
//
// Stream is false, so this blocks until the whole answer is ready. Streaming
// tokens down the WebSocket as they're produced is a Phase 5 concern.
func (c *Client) Chat(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	body, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Stream:  false,
		Options: options{Temperature: 0.2, NumPredict: maxOutputTokens},
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("chat request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("chat: ollama returned status %d", resp.StatusCode)
	}

	var cr chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", fmt.Errorf("chat decode: %w", err)
	}
	if cr.Message.Content == "" {
		return "", fmt.Errorf("chat: ollama returned an empty reply")
	}
	return cr.Message.Content, nil
}

// ChatStream is Chat, except it hands you the answer in pieces as the model
// produces them instead of making you wait for the whole thing.
//
// Since generation is strictly serial at a few tokens per second, waiting for a
// complete answer means staring at nothing for the entire duration. Streaming
// doesn't make it faster — total time is identical — it makes the FIRST word
// arrive in about two seconds instead of at the very end.
//
// onChunk is called once per fragment, in order. If it returns an error, we stop
// reading and abandon the request: that's how a disappeared client cancels work
// nobody will ever see, instead of us generating into the void.
func (c *Client) ChatStream(ctx context.Context, systemPrompt, userPrompt string, onChunk func(string) error) error {
	body, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Stream:  true, // <- the only difference in the request
		Options: options{Temperature: 0.2, NumPredict: maxOutputTokens},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("chat stream request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("chat stream: ollama returned status %d", resp.StatusCode)
	}

	// With stream:true Ollama replies with NEWLINE-DELIMITED JSON: one complete
	// object per fragment, one after another, rather than a single array. That
	// matters — an array couldn't be parsed until its closing bracket arrived,
	// which would defeat the whole point.
	//
	// json.Decoder reads from an io.Reader incrementally: each Decode pulls just
	// enough bytes for the next value and leaves the rest in the stream. Calling
	// it in a loop walks the response as it arrives over the wire.
	dec := json.NewDecoder(resp.Body)
	for {
		var cr chatResponse
		if err := dec.Decode(&cr); err != nil {
			if errors.Is(err, io.EOF) {
				return nil // stream ended cleanly
			}
			return fmt.Errorf("chat stream decode: %w", err)
		}
		if cr.Message.Content != "" {
			if err := onChunk(cr.Message.Content); err != nil {
				return err // caller wants out; the deferred Close abandons the request
			}
		}
		if cr.Done {
			return nil
		}
	}
}
