using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using ClickHouse.Driver;
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using OpenAI;
using Testcontainers.ClickHouse;

// ─── Configuration ───────────────────────────────────────────────────────────
//
// Defaults to Ollama (free, local). To use OpenAI instead:
//   OPENAI_BASE_URL=https://api.openai.com/v1  OPENAI_API_KEY=sk-...  EMBEDDING_MODEL=text-embedding-3-small
//
// Prerequisites:
//   1. Docker (for ClickHouse via Testcontainers)
//   2. Ollama with an embedding model:  ollama pull nomic-embed-text

const string datasetUrl = "https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies-2010s.json";
const int batchSize = 50; // Ollama handles smaller batches better

// ─── Main ────────────────────────────────────────────────────────────────────

var (generator, dims) = await SetupEmbeddingGeneratorAsync();
var (connectionString, container) = await SetupClickHouseAsync();

try
{
    var movies = await LoadMoviesAsync();
    var collection = await CreateCollectionAsync(connectionString, dims);
    await EnsureDataAsync(collection, generator, movies);
    await RunSearchLoopAsync(collection, generator);
}
finally
{
    if (container is not null)
    {
        Console.Write("Stopping ClickHouse... ");
        await container.StopAsync();
        Console.WriteLine("done.");
    }
}

return 0;

// ─── Setup ───────────────────────────────────────────────────────────────────

static async Task<(IEmbeddingGenerator<string, Embedding<float>> generator, int dims)> SetupEmbeddingGeneratorAsync()
{
    var baseUrl = Environment.GetEnvironmentVariable("OPENAI_BASE_URL") ?? "http://localhost:11434/v1";
    var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY") ?? "ollama";
    var modelId = Environment.GetEnvironmentVariable("EMBEDDING_MODEL") ?? "nomic-embed-text";

    IEmbeddingGenerator<string, Embedding<float>> generator = new OpenAIClient(
            new System.ClientModel.ApiKeyCredential(apiKey),
            new OpenAIClientOptions { Endpoint = new Uri(baseUrl) })
        .GetEmbeddingClient(modelId)
        .AsIEmbeddingGenerator();

    Console.Write($"Testing embedding model ({modelId})... ");
    var testEmbedding = await generator.GenerateAsync(["hello"]);
    var dims = testEmbedding[0].Vector.Length;
    Console.WriteLine($"OK ({dims} dimensions)");

    return (generator, dims);
}

static async Task<(string connectionString, ClickHouseContainer? container)> SetupClickHouseAsync()
{
    // Either:
    //   - Use CLICKHOUSE_CONNECTION_STRING if set (points to your own instance), or
    //   - Spin up a Testcontainers container with a persistent named volume so data
    //     survives between runs.

    var envConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING");

    if (envConnectionString is not null)
    {
        Console.WriteLine("Using ClickHouse from CLICKHOUSE_CONNECTION_STRING.");
        return (envConnectionString, null);
    }

    Console.Write("Starting ClickHouse container (with persistent volume)... ");
    var container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .WithVolumeMount("clickhouse-movie-search-data", "/var/lib/clickhouse")
        .Build();
    await container.StartAsync();
    Console.WriteLine("ready.");
    return (container.GetConnectionString(), container);
}

static async Task<List<MovieJson>> LoadMoviesAsync()
{
    var datasetPath = Path.GetFileName(new Uri(datasetUrl).LocalPath);

    if (!File.Exists(datasetPath))
    {
        Console.Write($"Downloading movie dataset to {datasetPath}... ");
        using var http = new HttpClient();
        var bytes = await http.GetByteArrayAsync(datasetUrl);
        await File.WriteAllBytesAsync(datasetPath, bytes);
        Console.WriteLine($"{bytes.Length / 1024} KB.");
    }
    else
    {
        Console.WriteLine($"Using cached dataset at {datasetPath}.");
    }

    Console.Write("Loading movies... ");
    await using var datasetStream = File.OpenRead(datasetPath);
    var movies = await JsonSerializer.DeserializeAsync(datasetStream, SourceGenerationContext.Default.ListMovieJson)
        ?? throw new InvalidOperationException("Failed to parse dataset");
    movies = movies.Where(m => m.Extract is { Length: > 50 }).ToList();
    Console.WriteLine($"{movies.Count} movies.");

    return movies;
}

static async Task<ClickHouseCollection<string, Movie>> CreateCollectionAsync(string connectionString, int dims)
{
    var store = new ClickHouseVectorStore(connectionString);

    var definition = new VectorStoreCollectionDefinition
    {
        Properties =
        [
            new VectorStoreKeyProperty("Key", typeof(string)),
            new VectorStoreDataProperty("Title", typeof(string)),
            new VectorStoreDataProperty("Year", typeof(int)) { IsIndexed = true },
            new VectorStoreDataProperty("Extract", typeof(string)),
            new VectorStoreDataProperty("Genres", typeof(string[])),
            new VectorStoreVectorProperty("Embedding", typeof(ReadOnlyMemory<float>), dims)
            {
                DistanceFunction = DistanceFunction.CosineDistance,
                IndexKind = IndexKind.Hnsw
            }
        ]
    };

    var collection = store.GetCollection<string, Movie>("movies", definition);
    await collection.EnsureCollectionExistsAsync();
    Console.WriteLine("Collection 'movies' ready.");
    return collection;
}

// ─── Data ingestion ──────────────────────────────────────────────────────────

static async Task EnsureDataAsync(
    ClickHouseCollection<string, Movie> collection,
    IEmbeddingGenerator<string, Embedding<float>> generator,
    List<MovieJson> movies)
{
    Console.Write("Checking for existing data... ");
    var client = (ClickHouseClient)collection.GetService(typeof(ClickHouseClient))!;
    var countResult = await client.ExecuteScalarAsync($"SELECT count() FROM `{collection.Name}` FINAL");
    int existingCount = Convert.ToInt32(countResult, CultureInfo.InvariantCulture);

    if (existingCount > 0)
    {
        Console.WriteLine($"found {existingCount} records.");
        if (existingCount == movies.Count)
        {
            Console.WriteLine("Collection matches current dataset — skipping embedding step.");
            return;
        }

        Console.Write($"Dataset has {movies.Count} movies. Re-generate embeddings? [y/N]: ");
        var response = Console.ReadLine()?.Trim();
        if (response is null || !response.StartsWith("y", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        Console.WriteLine("Dropping existing collection...");
        await collection.EnsureCollectionDeletedAsync();
        await collection.EnsureCollectionExistsAsync();
    }
    else
    {
        Console.WriteLine("none.");
    }

    await EmbedAndInsertAsync(collection, generator, movies);

    // Wait for the vector index to build
    await Task.Delay(1000);
}

static async Task EmbedAndInsertAsync(
    ClickHouseCollection<string, Movie> collection,
    IEmbeddingGenerator<string, Embedding<float>> generator,
    List<MovieJson> movies)
{
    Console.WriteLine($"Embedding and inserting {movies.Count} movies...");

    var total = movies.Count;
    var inserted = 0;

    for (int i = 0; i < total; i += batchSize)
    {
        var batch = movies.Skip(i).Take(batchSize).ToList();
        var texts = batch.Select(m => m.Extract!).ToList();

        var embeddings = await generator.GenerateAsync(texts);

        var records = new List<Movie>(batch.Count);
        for (int j = 0; j < batch.Count; j++)
        {
            var m = batch[j];
            records.Add(new Movie
            {
                Key = $"{m.Title}_{m.Year}",
                Title = m.Title ?? "",
                Year = m.Year,
                Extract = m.Extract ?? "",
                Genres = m.Genres ?? [],
                Embedding = embeddings[j].Vector
            });
        }

        await collection.UpsertAsync(records);
        inserted += batch.Count;

        var pct = (int)(100.0 * inserted / total);
        var bar = new string('#', pct / 4) + new string('.', 25 - pct / 4);
        Console.Write($"\r  [{bar}] {inserted}/{total} ({pct}%)");
    }
    Console.WriteLine("\nDone!");
}

// ─── Interactive search ──────────────────────────────────────────────────────

static async Task RunSearchLoopAsync(
    ClickHouseCollection<string, Movie> collection,
    IEmbeddingGenerator<string, Embedding<float>> generator)
{
    Console.WriteLine();
    Console.WriteLine("=== Semantic Movie Search ===");
    Console.WriteLine("Type a search query, or:");
    Console.WriteLine("  genre:<name>   — filter by genre (e.g. genre:Comedy)");
    Console.WriteLine("  year:<range>   — filter by year  (e.g. year:2015 or year:2013-2016)");
    Console.WriteLine("  top:<n>        — number of results (default: 5)");
    Console.WriteLine("  Example: robots and artificial intelligence genre:Sci-Fi year:2014-2018");
    Console.WriteLine();

    while (true)
    {
        Console.Write("> ");
        var input = Console.ReadLine()?.Trim();
        if (string.IsNullOrEmpty(input) || input is "quit" or "exit" or "q")
            break;

        var (query, genreFilter, yearFrom, yearTo, top) = ParseInput(input);

        if (string.IsNullOrWhiteSpace(query))
        {
            Console.WriteLine("Please enter a search query.");
            continue;
        }

        var queryEmbedding = await generator.GenerateAsync([query]);
        var queryVector = queryEmbedding[0].Vector;

        VectorSearchOptions<Movie>? searchOptions = null;
        if (genreFilter is not null || yearFrom is not null)
        {
            searchOptions = new VectorSearchOptions<Movie>
            {
                Filter = BuildFilter(genreFilter, yearFrom, yearTo)
            };
        }

        var results = new List<VectorSearchResult<Movie>>();
        await foreach (var result in collection.SearchAsync(queryVector, top, searchOptions))
        {
            results.Add(result);
        }

        PrintResults(results);
    }
}

static void PrintResults(List<VectorSearchResult<Movie>> results)
{
    if (results.Count == 0)
    {
        Console.WriteLine("No results found.");
        return;
    }

    Console.WriteLine();
    for (int i = 0; i < results.Count; i++)
    {
        var r = results[i];
        var genres = r.Record.Genres.Length > 0
            ? string.Join(", ", r.Record.Genres)
            : "Unknown";
        var score = 1.0 - r.Score; // convert distance to similarity
        Console.WriteLine($"  {i + 1}. {r.Record.Title} ({r.Record.Year}) [{genres}]  sim={score:F3}");

        var extract = r.Record.Extract.Length > 200
            ? r.Record.Extract[..200] + "..."
            : r.Record.Extract;
        Console.WriteLine($"     {extract}");
        Console.WriteLine();
    }
}

// ─── Query parsing ───────────────────────────────────────────────────────────

static (string query, string? genre, int? yearFrom, int? yearTo, int top) ParseInput(string input)
{
    string? genre = null;
    int? yearFrom = null;
    int? yearTo = null;
    int top = 5;
    var queryParts = new List<string>();

    foreach (var token in input.Split(' ', StringSplitOptions.RemoveEmptyEntries))
    {
        if (token.StartsWith("genre:", StringComparison.OrdinalIgnoreCase))
        {
            genre = token[6..];
        }
        else if (token.StartsWith("year:", StringComparison.OrdinalIgnoreCase))
        {
            var yearStr = token[5..];
            if (yearStr.Contains('-'))
            {
                var parts = yearStr.Split('-');
                if (int.TryParse(parts[0], out var from)) yearFrom = from;
                if (int.TryParse(parts[1], out var to)) yearTo = to;
            }
            else if (int.TryParse(yearStr, out var y))
            {
                yearFrom = y;
                yearTo = y;
            }
        }
        else if (token.StartsWith("top:", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(token[4..], out var t)) top = t;
        }
        else
        {
            queryParts.Add(token);
        }
    }

    return (string.Join(' ', queryParts), genre, yearFrom, yearTo, top);
}

static Expression<Func<Movie, bool>> BuildFilter(string? genre, int? yearFrom, int? yearTo)
{
    if (genre is not null && yearFrom is not null && yearTo is not null)
    {
        if (yearFrom == yearTo)
            return m => m.Genres.Contains(genre) && m.Year == yearFrom.Value;
        else
            return m => m.Genres.Contains(genre) && m.Year >= yearFrom.Value && m.Year <= yearTo.Value;
    }
    else if (genre is not null)
    {
        return m => m.Genres.Contains(genre);
    }
    else if (yearFrom is not null && yearTo is not null)
    {
        if (yearFrom == yearTo)
            return m => m.Year == yearFrom.Value;
        else
            return m => m.Year >= yearFrom.Value && m.Year <= yearTo.Value;
    }
    else if (yearFrom is not null)
    {
        return m => m.Year >= yearFrom.Value;
    }

    return m => true;
}

// ─── Models ──────────────────────────────────────────────────────────────────

public class Movie
{
    public string Key { get; set; } = "";
    public string Title { get; set; } = "";
    public int Year { get; set; }
    public string Extract { get; set; } = "";
    public string[] Genres { get; set; } = [];
    public ReadOnlyMemory<float> Embedding { get; set; }
}

public class MovieJson
{
    [JsonPropertyName("title")]
    public string? Title { get; set; }

    [JsonPropertyName("year")]
    public int Year { get; set; }

    [JsonPropertyName("extract")]
    public string? Extract { get; set; }

    [JsonPropertyName("genres")]
    public string[]? Genres { get; set; }
}

[JsonSerializable(typeof(List<MovieJson>))]
internal partial class SourceGenerationContext : JsonSerializerContext;
