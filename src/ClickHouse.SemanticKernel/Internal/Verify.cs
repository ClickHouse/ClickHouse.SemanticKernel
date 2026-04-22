using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ClickHouse.SemanticKernel;

internal static class Verify
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotNull([NotNull] object? value, [CallerArgumentExpression(nameof(value))] string? paramName = null)
    {
        if (value is null)
        {
            throw new ArgumentNullException(paramName);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotNullOrWhiteSpace([NotNull] string? value, [CallerArgumentExpression(nameof(value))] string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null, an empty string, or composed entirely of whitespace.", paramName);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void NotLessThan<T>(T value, T min, [CallerArgumentExpression(nameof(value))] string? paramName = null)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) < 0)
        {
            throw new ArgumentOutOfRangeException(paramName, value, $"The value must not be less than {min}.");
        }
    }
}
