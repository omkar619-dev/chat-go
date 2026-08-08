# Semantic search — design, and what it's bad at

Search-by-meaning over a room's message history. Both sides of the comparison go
through the same embedding model, so "nearest vector" means "closest meaning" —
which lets a query match a message that shares no words with it.

## How it works

```
gateway --(dual-write)--> Kafka chat.messages
                              |
              +---------------+---------------+
              |                               |
      persister group                   indexer group
              |                               |
      messages table                  Ollama /api/embeddings
      (history)                               |
                                     message_embeddings
                                     (384-dim vector + HNSW)
                                              |
                              GET /rooms/{id}/search?q=...
                              (embeds the query, cosine-ranks)
```

- **Model:** `all-minilm` via Ollama — 23M parameters, 384 dimensions, 512-token context.
- **Distance:** cosine (`<=>`), which compares the *angle* between vectors and so
  measures direction-of-meaning rather than magnitude. The HNSW index is built with
  `vector_cosine_ops` to match; a mismatched ops class would silently cause a full scan.
- **Independence:** the indexer is its own Kafka consumer group. It shares nothing
  with the persister, back-fills from offset 0 on first run, and can be wiped and
  rebuilt from the log. Adding search required no change to the gateway's hot path
  or to the persister.
- **Idempotency:** `UNIQUE (room_id, user_id, sent_at)` + `ON CONFLICT DO NOTHING`,
  because at-least-once delivery means the indexer can legitimately see a message twice.
- **Degradation:** the gateway calls Ollama only to embed a search *query*. If Ollama
  is down, `/search` returns **503** and live chat is unaffected.

## Measured behaviour (2026-07-26, 8-message corpus)

Two queries, run against 8 short test messages:

| Query | Top hit | Score | Verdict |
|---|---|---|---|
| `greeting` | "wassup brother! how u doin" | 31% | **Correct** — zero shared words |
| `no data was lost` | "the persister is down intentionally now bro!" | 34% | **Wrong** — see negation below |

The pipeline is correct: ordering is right, scores are sane, the only greeting in the
corpus won the `greeting` query. The *retrieval quality* is mediocre, for reasons below.

## Known limitations

### L1. Negation is nearly invisible to the model
`"no data was lost"` and `"data was lost"` embed almost identically — the word "no"
barely moves the vector while completely inverting the meaning. The `no data was lost`
query therefore behaved as though it asked for *"data was lost"* and returned the most
failure-flavoured messages. This is a well-known property of sentence embeddings, not a
bug in this code. It is the single strongest argument for hybrid retrieval (L4).

### L2. Short, generic messages sit near the centre of embedding space
Strings like `"wow a new msg"` are low-content, so they land near the centroid and score
mediocrely against *everything* — acting as background noise in every result list. They
took 2nd and 3rd place on the `greeting` query.

### L3. Score separation depends on whether the corpus has relevant content at all
*Revised 2026-07-27 — the original conclusion here ("no cliff, a threshold cannot help")
was over-generalised from a single query.*

Two queries, very different shapes:

| Query | Distribution | Cliff? |
|---|---|---|
| `greeting` | 31 / 25 / 22 / 17 / 15 / 13 / 7 / 0 | **No** — smooth slope |
| `what happened with the persister?` | 80 / 79 / 74 / 47 / **16** / 15 / 11 / 11 / 6 | **Yes** — 47 → 16 |

The difference is the corpus, not the model. `greeting` matches exactly ONE message, so
the correct hit has nothing to separate from and sits only 6 points above generic filler.
The persister query matches FOUR genuinely related messages, and the gap between the last
real hit (47%) and the first noise (16%) is unmistakable.

**Implication:** a similarity threshold *is* worth having, and `0.30` sits inside the
observed gap. It is now applied in the RAG bot (`cmd/bot`, `minSimilarity`) — where its
value is concrete rather than cosmetic, because irrelevant context costs prompt-eval time
on slow hardware. It is deliberately still NOT applied to the `/search` API, where showing
a ranked list including weak matches is reasonable behaviour.

Caveat that still stands: `LIMIT 10` over ~9 indexed messages means the API returns
essentially everything, so only the *ordering* carries information there.

### L4. Vector-only retrieval — no keyword channel
Production retrieval blends dense (vector) with sparse (BM25 / Postgres
`to_tsvector` + `websearch_to_tsquery`) search, because vector search fumbles negation,
proper nouns, rare tokens and exact identifiers, while keyword search handles those and
fails at paraphrase. **Deferred deliberately:** hybrid means tuning blend weights, and
weights tuned against an 8-message corpus are invented, not measured. Needs L6 first.

### L5. Small model
`all-minilm` (23M params) is fast and cheap — appropriate for a Core-2-Duo homelab box —
but weak at propositional meaning. `nomic-embed-text` (768-dim) would likely be better,
at the cost of changing `vector(384)`, rebuilding the HNSW index, and re-indexing the
whole log. **Not yet justified:** it is not established that the model, rather than the
tiny slangy corpus, is the limiting factor. Changing it now would be guessing.

### L6. No evaluation harness — the real blocker
There is no fixed query set with expected results, so there is no way to tell whether a
change to retrieval helped or hurt. Every quality judgement above rests on eight ad-hoc
test messages, which is not enough to conclude anything. **This is the prerequisite for
L3, L4 and L5** — news-feed's `eval/` directory is the model to copy.

### L7. No re-index path for a model change
Changing the embedding model silently invalidates every stored vector (different model =
different space; comparisons become meaningless). Recovery today means truncating
`message_embeddings` and resetting the `indexer` consumer group offset to zero. Worth a
documented command, or a `model` column so mixed-model rows are detectable.

## Order of work when returning to this

1. **L6 — eval harness.** A seeded corpus of realistic messages plus a query→expected-hit
   set. Without this the rest is unmeasurable.
2. **L4 — hybrid retrieval**, tuned against that eval.
3. **L3 — threshold**, once separation exists to threshold on.
4. **L5 — larger model**, only if the eval shows the model is the bottleneck.
