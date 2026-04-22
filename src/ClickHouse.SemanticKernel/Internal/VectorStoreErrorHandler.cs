using System;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Extensions.VectorData;

#pragma warning disable MEVD9000 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.

/// <summary>
/// Wraps vector-store operations so that driver exceptions surface as <see cref="VectorStoreException"/>.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class VectorStoreErrorHandler
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<TResult> RunOperationAsync<TResult, TException>(
        VectorStoreMetadata metadata,
        string operationName,
        Func<Task<TResult>> operation)
        where TException : Exception
    {
        return RunOperationAsync<TResult, TException>(
            new VectorStoreCollectionMetadata
            {
                CollectionName = null,
                VectorStoreName = metadata.VectorStoreName,
                VectorStoreSystemName = metadata.VectorStoreSystemName,
            },
            operationName,
            operation);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<TResult> RunOperationAsync<TResult, TException>(
        VectorStoreCollectionMetadata metadata,
        string operationName,
        Func<Task<TResult>> operation)
        where TException : Exception
    {
        try
        {
            return await operation.Invoke().ConfigureAwait(false);
        }
        catch (AggregateException ex) when (ex.InnerException is TException)
        {
            throw new VectorStoreException("Call to vector store failed.", ex)
            {
                VectorStoreSystemName = metadata.VectorStoreSystemName,
                VectorStoreName = metadata.VectorStoreName,
                CollectionName = metadata.CollectionName,
                OperationName = operationName
            };
        }
        catch (TException ex)
        {
            throw new VectorStoreException("Call to vector store failed.", ex)
            {
                VectorStoreSystemName = metadata.VectorStoreSystemName,
                VectorStoreName = metadata.VectorStoreName,
                CollectionName = metadata.CollectionName,
                OperationName = operationName
            };
        }
    }

    internal static Task<bool> ReadWithErrorHandlingAsync(
        this DbDataReader reader,
        VectorStoreCollectionMetadata metadata,
        string operationName,
        CancellationToken cancellationToken)
        => RunOperationAsync<bool, DbException>(
            metadata,
            operationName,
            () => reader.ReadAsync(cancellationToken));

    internal static Task<bool> ReadWithErrorHandlingAsync(
        this DbDataReader reader,
        VectorStoreMetadata metadata,
        string operationName,
        CancellationToken cancellationToken)
        => RunOperationAsync<bool, DbException>(
            metadata,
            operationName,
            () => reader.ReadAsync(cancellationToken));
}
