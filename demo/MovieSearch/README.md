# MovieSearch

A runnable demo for `ClickHouse.SemanticKernel`. Embeds ~2.5k Wikipedia movie summaries (2010s), indexes them in ClickHouse with an HNSW vector index, and exposes an interactive semantic-search REPL with optional genre/year filters.

## Prerequisites

- .NET 10 SDK (pinned in `global.json` at the repo root; the published library targets `net8.0`)
- Docker (for the ClickHouse container)
- [Ollama](https://ollama.com/) with an embedding model:
  ```
  ollama pull nomic-embed-text
  ```

## Run

```
dotnet run
```

On first launch this will:

1. Start a ClickHouse container via Testcontainers (data persists in the Docker named volume `clickhouse-movie-search-data`).
2. Download the movie dataset to `movies-2010s.json` if it's not already present.
3. Embed every movie's extract with `nomic-embed-text` and upsert into the `movies` collection.
4. Drop you into an interactive prompt.

On subsequent runs, steps 2-3 are skipped — it reuses the existing embeddings unless you opt to regenerate.

## Query syntax

```
> time travel and paradoxes
> robots and artificial intelligence genre:Sci-Fi year:2014-2018
> heist movies top:10
```

- `genre:<name>` — filter by genre (translated to `has(Genres, ...)`)
- `year:<n>` or `year:<from>-<to>` — filter by year (translated to `>=`/`<=`)
- `top:<n>` — number of results (default 5)
- `quit` / `exit` / `q` — stop

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `CLICKHOUSE_CONNECTION_STRING` | — | Use your own ClickHouse instead of a container |
| `OPENAI_BASE_URL` | `http://localhost:11434/v1` | Embedding endpoint (Ollama by default) |
| `OPENAI_API_KEY` | `ollama` | Required by the OpenAI client but unused by Ollama |
| `EMBEDDING_MODEL` | `nomic-embed-text` | Model name |

To use OpenAI instead of Ollama:

```
OPENAI_BASE_URL=https://api.openai.com/v1 \
OPENAI_API_KEY=sk-... \
EMBEDDING_MODEL=text-embedding-3-small \
dotnet run
```
