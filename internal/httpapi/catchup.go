package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// catchupCommand is typed into the message box like any chat message, but the
// gateway intercepts it instead of publishing it. A slash command needs no
// protocol change on the way IN — the socket still just carries text — and it's
// the convention every chat app already trained users on.
const catchupCommand = "/catchup"

// catchupSystemPrompt tells the model what kind of summary we want. "Only use
// what is in the transcript" is the same grounding rule the RAG bot uses, and for
// the same reason: a small model will happily embellish if you don't forbid it.
const catchupSystemPrompt = `You are summarising a chat room for someone who has been away.
Summarise what was discussed as three or four short bullet points.
Only use what is in the transcript — do not invent details or add advice.
No preamble, no sign-off, no restating this instruction.`

// streamCatchup summarises what THIS user missed and streams it down their one
// socket, a fragment at a time.
//
// Two things make this different from the RAG bot, and both follow from the same
// fact: a summary is PERSONAL. It answers "what did I miss?", so it belongs to
// the person who asked.
//
//  1. It runs in the GATEWAY, not in cmd/bot. The bot publishes to Redis and
//     Kafka, which would broadcast the summary to the whole room and persist it
//     forever — and streaming through that path would store a row per fragment.
//  2. Nothing is published or persisted. The summary exists only for the duration
//     of this write. It's derived data; the messages it came from are the record.
//
// Runs in its own goroutine so the read loop keeps accepting messages while a
// summary streams. That's safe because coder/websocket serialises writes
// internally ("All methods may be called concurrently except for Reader and
// Read"), so this and the Redis pump can both write without coordination.
func (h *Handlers) streamCatchup(ctx context.Context, conn *websocket.Conn, roomID, userID int64) {
	// 1. Where did this user leave off? The watermark is written when their socket
	//    closes, so it marks the moment they stopped being present in the room.
	lastRead, err := h.Queries.GetRoomRead(ctx, sqlc.GetRoomReadParams{RoomID: roomID, UserID: userID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No watermark at all: they've never had a socket open here, so there is no
		// "gap" to describe. Fall back to the last 50 — the right answer for a
		// first visit, and exactly what this feature did before 5.4.
		h.summariseRecent(ctx, conn, roomID, userID)
		return
	case err != nil:
		log.Printf("catchup watermark (room %d, user %d): %v", roomID, userID, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not read your position in this room"})
		return
	}

	// 2. How much did they actually miss? UserID excludes their own messages —
	//    you have read what you wrote.
	total, err := h.Queries.CountUnreadMessages(ctx, sqlc.CountUnreadMessagesParams{
		RoomID: roomID, CreatedAt: lastRead, UserID: userID,
	})
	if err != nil {
		log.Printf("catchup count (room %d, user %d): %v", roomID, userID, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not read this room's history"})
		return
	}

	// THIS IS THE POINT OF 5.4. With a watermark we can finally give the correct
	// answer to "what did I miss?" when the answer is "nothing". Before this,
	// /catchup always summarised the last 50 messages — including the ones you
	// had just finished reading.
	if total == 0 {
		// Logged like the other outcomes. This branch is the cheapest and most
		// common one, and until it said so there was no way to tell "answered
		// instantly" apart from "never ran" when reading the gateway's output.
		log.Printf("catchup (room %d, user %d): nothing unread", roomID, userID)
		_ = writeFrame(ctx, conn, frame{Type: frameSummaryChunk,
			Text: "You're all caught up — nothing new since you were last here."})
		_ = writeFrame(ctx, conn, frame{Type: frameSummaryDone})
		return
	}

	unread, err := h.Queries.ListUnreadMessages(ctx, sqlc.ListUnreadMessagesParams{
		RoomID: roomID, CreatedAt: lastRead, UserID: userID,
	})
	if err != nil || len(unread) == 0 {
		log.Printf("catchup unread (room %d, user %d): %v", roomID, userID, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not read this room's history"})
		return
	}

	// Be honest when the 200-message cap bit, instead of silently truncating.
	if int64(len(unread)) < total {
		_ = writeFrame(ctx, conn, frame{Type: frameSummaryChunk,
			Text: fmt.Sprintf("(%d unread — summarising the most recent %d)\n\n", total, len(unread))})
	}

	var b strings.Builder
	b.WriteString("Chat transcript, oldest first:\n\n")
	for _, m := range unread {
		fmt.Fprintf(&b, "%s: %s\n", m.Username, m.Body)
	}
	h.streamSummary(ctx, conn, roomID, userID, b.String(), len(unread))
}

// summariseRecent is the pre-5.4 behaviour: summarise the last 50 messages,
// regardless of who's asking. Kept as the fallback for a user with no read
// watermark yet, where "what did you miss" has no meaningful answer.
func (h *Handlers) summariseRecent(ctx context.Context, conn *websocket.Conn, roomID, userID int64) {
	history, err := h.Queries.ListRecentMessages(ctx, roomID)
	if err != nil {
		log.Printf("catchup history (room %d, user %d): %v", roomID, userID, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not read this room's history"})
		return
	}
	if len(history) == 0 {
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "there's nothing here to summarise yet"})
		return
	}

	var b strings.Builder
	b.WriteString("Chat transcript, oldest first:\n\n")
	for _, m := range history {
		fmt.Fprintf(&b, "%s: %s\n", m.Username, m.Body)
	}
	h.streamSummary(ctx, conn, roomID, userID, b.String(), len(history))
}

// streamSummary runs the model and pumps each fragment down the socket as it's
// produced. Shared by both paths above so there's one place that talks to the LLM.
//
// userID is carried purely for the log lines. A summary is per-user, so a room id
// alone can't tell two concurrent catch-ups apart — and once there's more than one
// gateway process, reading these logs is the only way to follow a single request.
func (h *Handlers) streamSummary(ctx context.Context, conn *websocket.Conn, roomID, userID int64, transcript string, count int) {
	genCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()

	started := time.Now()
	err := h.Generator.ChatStream(genCtx, catchupSystemPrompt, transcript, func(chunk string) error {
		// Returning an error here aborts the whole generation. That's the point:
		// if this write fails the client is gone, and there's no reason to keep
		// occupying the GPU producing a summary nobody will read.
		return writeFrame(ctx, conn, frame{Type: frameSummaryChunk, Text: chunk})
	})
	if err != nil {
		log.Printf("catchup (room %d, user %d): %v", roomID, userID, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "the summary failed partway through"})
		return
	}

	log.Printf("catchup (room %d, user %d): summarised %d messages in %s",
		roomID, userID, count, time.Since(started).Round(time.Second))
	_ = writeFrame(ctx, conn, frame{Type: frameSummaryDone})
}
