<p align="center">
<h1 align="center">ClickHouse.SemanticKernel</h1>
</p>
<br/>
<p align="center">

<a href="https://www.nuget.org/packages/ClickHouse.SemanticKernel">
<img alt="NuGet Version" src="https://img.shields.io/nuget/v/ClickHouse.SemanticKernel?label=ClickHouse.SemanticKernel">
</a>

<a href="https://www.nuget.org/packages/ClickHouse.SemanticKernel">
<img alt="NuGet Downloads" src="https://img.shields.io/nuget/dt/ClickHouse.SemanticKernel">
</a>

<a href="https://github.com/ClickHouse/ClickHouse.SemanticKernel/actions/workflows/tests.yml">
<img src="https://github.com/ClickHouse/ClickHouse.SemanticKernel/actions/workflows/tests.yml/badge.svg?branch=main">
</a>

<a href="https://codecov.io/gh/ClickHouse/ClickHouse.SemanticKernel">
<img src="https://codecov.io/gh/ClickHouse/ClickHouse.SemanticKernel/graph/badge.svg">
</a>

</p>

A Semantic Kernel connector for [ClickHouse](https://clickhouse.com/), built on [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs). Use ClickHouse as a vector store from [Semantic Kernel](https://learn.microsoft.com/semantic-kernel/overview/) and any other `Microsoft.Extensions.VectorData` consumer. CRUD, filtered queries, and vector similarity search through the standard SK interface.

## Features

- **Vector similarity search** via the native [`vector_similarity`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/annindexes) HNSW index with `cosineDistance` or `L2Distance`
- **LINQ-based filters** translated to ClickHouse SQL, including `has()` / `hasAny()` for array membership, null-safe equality, and range predicates
- **Upsert semantics** on top of `ReplacingMergeTree` with `FINAL` reads for immediate consistency
- **Dynamic collections** (`Dictionary<string, object?>`) alongside strongly-typed records
- **Client-side embedding generation** via `IEmbeddingGenerator<string, Embedding<float>>`. Set a generator at the store, collection, or property level
- **DI integration** with `AddClickHouseVectorStore` / `AddClickHouseCollection` extensions

## Semantic Kernel

**[Semantic Kernel (SK)](https://learn.microsoft.com/semantic-kernel/overview/)** is Microsoft's open-source SDK for building AI agents in C#, Python, and Java. The Kernel holds AI services (chat completion, embeddings), plugins (your own code or OpenAPI endpoints exposed as callable functions), and memory. Agents use the kernel to plan, call tools, and retrieve context; when an LLM needs facts it doesn't have baked in, SK pulls them from a vector store and stuffs them into the prompt. That's the core RAG loop.

**This connector** implements Microsoft.Extensions.VectorData  for ClickHouse. If you're using Semantic Kernel, register it and your agents can use ClickHouse as long-term memory. If you're not using SK, you still get a clean .NET API for vector CRUD and search backed by ClickHouse's `vector_similarity` HNSW index.

### Typical use cases

- **RAG over a knowledge base.** Embed docs/wiki pages/support tickets into ClickHouse, then at query time embed the user's question, retrieve the top-k nearest chunks, and feed them to the LLM as context. ClickHouse's column-store excels when the corpus has metadata (timestamps, authors, product areas) that you want to filter on alongside vector similarity.
- **Agent long-term memory.** Store conversation summaries, user preferences, or tool-call results as embeddings. The agent retrieves relevant memories each turn via MEVD's `SearchAsync`.
- **Semantic search over structured data.** Product catalogs, media libraries, research papers, anything where a natural-language query should match on meaning, not keywords. Combine with ClickHouse's analytical queries for faceted search.
- **Hybrid pipelines.** Run vector search to get a candidate set, then use standard ClickHouse SQL (aggregations, joins, window functions) to rerank or aggregate. Because the vector column lives in the same table as your metadata, there's no cross-system join.



## Getting started

### Define your record

```csharp
using Microsoft.Extensions.VectorData;

public class Movie
{
    [VectorStoreKey]
    public string Key { get; set; } = "";

    [VectorStoreData]
    public string Title { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public int Year { get; set; }

    [VectorStoreData]
    public string[] Genres { get; set; } = [];

    [VectorStoreData]
    public string Extract { get; set; } = "";

    [VectorStoreVector(Dimensions: 768, DistanceFunction = DistanceFunction.CosineDistance, IndexKind = IndexKind.Hnsw)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}
```

Supported key types: `int`, `long`, `string`, `Guid`.
Supported vector types: `ReadOnlyMemory<float>`, `Embedding<float>`, `float[]`.
Supported data types: `int`, `long`, `float`, `double`, `bool`, `string`, `Guid`, `DateTime`, `DateTimeOffset`, `string[]`, `List<string>`, `int[]`.

### Create a collection and insert

```csharp
using ClickHouse.SemanticKernel;

var store = new ClickHouseVectorStore("Host=localhost;Protocol=http;Database=default");
var collection = store.GetCollection<string, Movie>("movies");

await collection.EnsureCollectionExistsAsync();

await collection.UpsertAsync(new Movie
{
    Key = "inception-2010",
    Title = "Inception",
    Year = 2010,
    Genres = ["Action", "Sci-Fi"],
    Extract = "A thief who steals corporate secrets through dream-sharing technology...",
    Embedding = myEmbedding
});
```

Under the hood the collection is created with `ENGINE = ReplacingMergeTree() ORDER BY (Key)` and a `vector_similarity` skip index on the vector column. Upserts are plain `INSERT`s; `FINAL` is applied to reads so you always see the latest row per key.

### Vector search

```csharp
var results = collection.SearchAsync(queryEmbedding, top: 5);

await foreach (var result in results)
{
    Console.WriteLine($"{result.Record.Title}  score={result.Score}");
}
```

### Filtered search

Combine vector similarity with LINQ filters:

```csharp
var results = collection.SearchAsync(
    queryEmbedding,
    top: 5,
    new VectorSearchOptions<Movie>
    {
        Filter = m => m.Year >= 2015 && m.Genres.Contains("Sci-Fi")
    });
```

`Contains` on an array/list is translated to ClickHouse `has()`; `Any` + `Contains` becomes `hasAny()`.

### Client-side embedding generation

Configure an `IEmbeddingGenerator` at the store level and let the connector embed strings on upsert and search:

```csharp
IEmbeddingGenerator<string, Embedding<float>> generator = /* OpenAI, Ollama, etc. */;

var store = new ClickHouseVectorStore(
    connectionString,
    new ClickHouseVectorStoreOptions { EmbeddingGenerator = generator });

// Now records can declare a `string` vector property — the connector will
// call the generator on upsert. Searches accept strings directly.
var results = collection.SearchAsync("time travel paradoxes", top: 5);
```

Precedence, highest to lowest: property-level generator > collection-definition generator > store-level generator.

### Dependency injection

```csharp
services.AddClickHouseVectorStore(_ => connectionString);
services.AddClickHouseCollection<string, Movie>("movies", _ => connectionString);
```

Registers `VectorStore`, `VectorStoreCollection<string, Movie>`, and `IVectorSearchable<Movie>`. If an `IEmbeddingGenerator` is registered in the container, it's picked up automatically.

### Using it from a Semantic Kernel agent

With the collection registered in DI, a Semantic Kernel agent can use it for RAG out of the box:

```csharp
var builder = Kernel.CreateBuilder();
builder.AddOpenAIChatCompletion("gpt-4o-mini", apiKey);
builder.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(embeddingGenerator);
builder.Services.AddClickHouseCollection<string, Movie>("movies", _ => connectionString);

var kernel = builder.Build();

// Expose the collection's search as a text search plugin the LLM can call.
var searchable = kernel.Services.GetRequiredService<IVectorSearchable<Movie>>();
kernel.Plugins.Add(searchable.CreateKernelPluginFromTextSearch("MovieSearch"));

// Ask the agent; it'll call MovieSearch when it needs context.
var result = await kernel.InvokePromptAsync("Recommend a movie about time travel.");
```

See the SK docs on [using vector stores for RAG](https://learn.microsoft.com/semantic-kernel/concepts/text-search/text-search-vector-stores) and [adding RAG to agents](https://learn.microsoft.com/semantic-kernel/frameworks/agent/agent-rag) for the full picture.

## Demo

The `demo/MovieSearch` directory contains a runnable console app that:

1. Starts a ClickHouse container via Testcontainers
2. Downloads ~2.5k Wikipedia movie summaries
3. Embeds them with a local [Ollama](https://ollama.com/) model (`nomic-embed-text`)
4. Runs an interactive search loop with optional `genre:` and `year:` filters

```
cd demo/MovieSearch
ollama pull nomic-embed-text
dotnet run
```

Example session:

```
> time travel and paradoxes
  1. Predestination (2014) [Sci-Fi, Thriller]  sim=0.82
  2. Looper (2012) [Action, Crime, Sci-Fi]     sim=0.79
  3. Project Almanac (2015) [Sci-Fi, Thriller] sim=0.76

> robots and artificial intelligence genre:Sci-Fi year:2014-2018
  ...
```

To use OpenAI instead of Ollama, set `OPENAI_BASE_URL=https://api.openai.com/v1`, `OPENAI_API_KEY=sk-...`, and `EMBEDDING_MODEL=text-embedding-3-small`.

## ClickHouse specifics

ClickHouse is append-oriented, so the connector handles mutability as follows:

| Operation | Mechanism |
|-----------|-----------|
| Insert / Upsert | `INSERT INTO ... VALUES`; `ReplacingMergeTree` keeps the latest row per key after background merges |
| Update | Same as upsert, re-inserts the full row with the same key |
| Delete | Lightweight `DELETE FROM ... WHERE` (mutation-backed) |
| Read | `SELECT ... FROM t FINAL` to force on-the-fly deduplication at query time |

The tradeoff is read-time CPU cost for `FINAL`, but this keeps semantics consistent without waiting on merges. For vector-store workloads (bulk ingest, many searches) this works well.

The `vector_similarity` index requires ClickHouse 25.2 or newer and uses the three-argument syntax `('hnsw', '<distance>', <dimensions>)`.

## License

Apache-2.0
