using System;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.SemanticKernel;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods to register <see cref="ClickHouseVectorStore"/> instances on an <see cref="IServiceCollection"/>.
/// </summary>
public static class ClickHouseServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="ClickHouseVectorStore"/> as <see cref="VectorStore"/>, with the specified connection string and service lifetime.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddClickHouseVectorStore(
        this IServiceCollection services,
        Func<IServiceProvider, string> connectionStringProvider,
        Func<IServiceProvider, ClickHouseVectorStoreOptions>? optionsProvider = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        => AddKeyedClickHouseVectorStore(services, serviceKey: null, connectionStringProvider, optionsProvider, lifetime);

    /// <summary>
    /// Registers a keyed <see cref="ClickHouseVectorStore"/> as <see cref="VectorStore"/>, with the specified connection string and service lifetime.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddKeyedClickHouseVectorStore(
        this IServiceCollection services,
        object? serviceKey,
        Func<IServiceProvider, string> connectionStringProvider,
        Func<IServiceProvider, ClickHouseVectorStoreOptions>? optionsProvider = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
    {
        Verify.NotNull(services);
        Verify.NotNull(connectionStringProvider);

        services.Add(new ServiceDescriptor(typeof(ClickHouseVectorStore), serviceKey, (sp, _) =>
        {
            var connectionString = connectionStringProvider(sp);
            var options = GetStoreOptions(sp, optionsProvider);
            return new ClickHouseVectorStore(connectionString, options);
        }, lifetime));

        services.Add(new ServiceDescriptor(typeof(VectorStore), serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<ClickHouseVectorStore>(key), lifetime));

        return services;
    }

    /// <summary>
    /// Registers a <see cref="ClickHouseCollection{TKey, TRecord}"/> as <see cref="VectorStoreCollection{TKey, TRecord}"/>.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddClickHouseCollection<TKey, TRecord>(
        this IServiceCollection services,
        string name,
        Func<IServiceProvider, string> connectionStringProvider,
        Func<IServiceProvider, ClickHouseCollectionOptions>? optionsProvider = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where TKey : notnull
        where TRecord : class
        => AddKeyedClickHouseCollection<TKey, TRecord>(services, serviceKey: null, name, connectionStringProvider, optionsProvider, lifetime);

    /// <summary>
    /// Registers a keyed <see cref="ClickHouseCollection{TKey, TRecord}"/> as <see cref="VectorStoreCollection{TKey, TRecord}"/>.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddKeyedClickHouseCollection<TKey, TRecord>(
        this IServiceCollection services,
        object? serviceKey,
        string name,
        Func<IServiceProvider, string> connectionStringProvider,
        Func<IServiceProvider, ClickHouseCollectionOptions>? optionsProvider = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where TKey : notnull
        where TRecord : class
    {
        Verify.NotNull(services);
        Verify.NotNullOrWhiteSpace(name);
        Verify.NotNull(connectionStringProvider);

        services.Add(new ServiceDescriptor(typeof(ClickHouseCollection<TKey, TRecord>), serviceKey, (sp, _) =>
        {
            var connectionString = connectionStringProvider(sp);
            var options = GetCollectionOptions(sp, optionsProvider);
            return new ClickHouseCollection<TKey, TRecord>(connectionString, name, options);
        }, lifetime));

        services.Add(new ServiceDescriptor(typeof(VectorStoreCollection<TKey, TRecord>), serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<ClickHouseCollection<TKey, TRecord>>(key), lifetime));

        services.Add(new ServiceDescriptor(typeof(IVectorSearchable<TRecord>), serviceKey,
            static (sp, key) => sp.GetRequiredKeyedService<ClickHouseCollection<TKey, TRecord>>(key), lifetime));

        return services;
    }

    /// <summary>
    /// Registers a <see cref="ClickHouseCollection{TKey, TRecord}"/> with a static connection string.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddClickHouseCollection<TKey, TRecord>(
        this IServiceCollection services,
        string name,
        string connectionString,
        ClickHouseCollectionOptions? options = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where TKey : notnull
        where TRecord : class
        => AddKeyedClickHouseCollection<TKey, TRecord>(services, serviceKey: null, name, connectionString, options, lifetime);

    /// <summary>
    /// Registers a keyed <see cref="ClickHouseCollection{TKey, TRecord}"/> with a static connection string.
    /// </summary>
    [RequiresUnreferencedCode("The ClickHouse provider is currently incompatible with trimming.")]
    [RequiresDynamicCode("The ClickHouse provider is currently incompatible with NativeAOT.")]
    public static IServiceCollection AddKeyedClickHouseCollection<TKey, TRecord>(
        this IServiceCollection services,
        object? serviceKey,
        string name,
        string connectionString,
        ClickHouseCollectionOptions? options = null,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where TKey : notnull
        where TRecord : class
    {
        Verify.NotNullOrWhiteSpace(connectionString);

        return AddKeyedClickHouseCollection<TKey, TRecord>(services, serviceKey, name, _ => connectionString, _ => options!, lifetime);
    }

    private static ClickHouseVectorStoreOptions? GetStoreOptions(IServiceProvider sp, Func<IServiceProvider, ClickHouseVectorStoreOptions?>? optionsProvider)
    {
        var options = optionsProvider?.Invoke(sp);
        if (options?.EmbeddingGenerator is not null)
        {
            return options;
        }

        var embeddingGenerator = sp.GetService<IEmbeddingGenerator>();
        return embeddingGenerator is null
            ? options
            : new(options) { EmbeddingGenerator = embeddingGenerator };
    }

    private static ClickHouseCollectionOptions? GetCollectionOptions(IServiceProvider sp, Func<IServiceProvider, ClickHouseCollectionOptions?>? optionsProvider)
    {
        var options = optionsProvider?.Invoke(sp);
        if (options?.EmbeddingGenerator is not null)
        {
            return options;
        }

        var embeddingGenerator = sp.GetService<IEmbeddingGenerator>();
        return embeddingGenerator is null
            ? options
            : new(options) { EmbeddingGenerator = embeddingGenerator };
    }
}
