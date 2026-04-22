# ClickHouse.SemanticKernel

A [Microsoft.Extensions.VectorData](https://learn.microsoft.com/dotnet/ai/microsoft-extensions-ai) connector for [ClickHouse](https://clickhouse.com/), built on [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs). Use ClickHouse as a vector store from Semantic Kernel and any other `Microsoft.Extensions.VectorData` consumer — CRUD, filtered queries, and vector similarity search through the standard SK interface.

## Features

- Vector similarity search via the native `vector_similarity` HNSW index with `cosineDistance` or `L2Distance`
- LINQ-based filters translated to ClickHouse SQL (`has()`, `hasAny()`, null-safe equality, range predicates)
- Upsert semantics on top of `ReplacingMergeTree` with `FINAL` reads for immediate consistency
- Dynamic collections (`Dictionary<string, object?>`) alongside strongly-typed records
- Client-side embedding generation via `IEmbeddingGenerator<string, Embedding<float>>`
- DI integration via `AddClickHouseVectorStore` / `AddClickHouseCollection`

## Usage

```csharp
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.VectorData;

public class Movie
{
    [VectorStoreKey] public string Key { get; set; } = "";
    [VectorStoreData] public string Title { get; set; } = "";
    [VectorStoreData(IsIndexed = true)] public int Year { get; set; }
    [VectorStoreData] public string[] Genres { get; set; } = [];

    [VectorStoreVector(Dimensions: 768, DistanceFunction = DistanceFunction.CosineDistance, IndexKind = IndexKind.Hnsw)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

var store = new ClickHouseVectorStore("Host=localhost;Protocol=http;Database=default");
var collection = store.GetCollection<string, Movie>("movies");
await collection.EnsureCollectionExistsAsync();

await collection.UpsertAsync(new Movie { /* ... */ });

var results = collection.SearchAsync(
    queryEmbedding,
    top: 5,
    new VectorSearchOptions<Movie>
    {
        Filter = m => m.Year >= 2015 && m.Genres.Contains("Sci-Fi")
    });
```

## Requirements

- .NET 8 runtime or newer (the package targets `net8.0`; building from source needs the .NET 10 SDK pinned in `global.json`)
- ClickHouse 25.2+ (for the `vector_similarity` skip index)

## More information

- [GitHub repository](https://github.com/ClickHouse/ClickHouse.SemanticKernel)
- [ClickHouse documentation](https://clickhouse.com/docs)
- [Microsoft.Extensions.VectorData documentation](https://learn.microsoft.com/dotnet/ai/microsoft-extensions-ai)
