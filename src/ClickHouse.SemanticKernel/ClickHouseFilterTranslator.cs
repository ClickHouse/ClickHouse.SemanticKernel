using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using ClickHouse.Driver.ADO.Parameters;
using Microsoft.Extensions.VectorData.ProviderServices;
using Microsoft.Extensions.VectorData.ProviderServices.Filter;

namespace ClickHouse.SemanticKernel;

internal sealed class ClickHouseFilterTranslator
{
    private CollectionModel _model = null!;
    private ParameterExpression _recordParameter = null!;
    private readonly StringBuilder _sql;
    private readonly ClickHouseParameterCollection _parameters;

    internal ClickHouseFilterTranslator(StringBuilder sql, ClickHouseParameterCollection parameters)
    {
        this._sql = sql;
        this._parameters = parameters;
    }

    internal void Translate(LambdaExpression lambdaExpression, CollectionModel model, bool appendWhere)
    {
        this._model = model;

        Debug.Assert(lambdaExpression.Parameters.Count == 1);
        this._recordParameter = lambdaExpression.Parameters[0];

        var preprocessor = new FilterTranslationPreprocessor { SupportsParameterization = false };
        var preprocessedExpression = preprocessor.Visit(lambdaExpression.Body);

        if (appendWhere)
        {
            this._sql.Append("WHERE ");
        }

        this.TranslateExpression(preprocessedExpression, isSearchCondition: true);
    }

    private void TranslateExpression(Expression? node, bool isSearchCondition = false)
    {
        switch (node)
        {
            case BinaryExpression binary:
                this.TranslateBinary(binary);
                return;

            case ConstantExpression constant:
                this.TranslateConstant(constant.Value, isSearchCondition);
                return;

            case MemberExpression member:
                this.TranslateMember(member, isSearchCondition);
                return;

            case MethodCallExpression methodCall:
                this.TranslateMethodCall(methodCall, isSearchCondition);
                return;

            case UnaryExpression unary:
                this.TranslateUnary(unary, isSearchCondition);
                return;

            default:
                throw new NotSupportedException("Unsupported NodeType in filter: " + node?.NodeType);
        }
    }

    private void TranslateBinary(BinaryExpression binary)
    {
        switch (binary.NodeType)
        {
            case ExpressionType.Equal when IsNull(binary.Right):
                this._sql.Append('(');
                this.TranslateExpression(binary.Left);
                this._sql.Append(" IS NULL)");
                return;
            case ExpressionType.NotEqual when IsNull(binary.Right):
                this._sql.Append('(');
                this.TranslateExpression(binary.Left);
                this._sql.Append(" IS NOT NULL)");
                return;
            case ExpressionType.Equal when IsNull(binary.Left):
                this._sql.Append('(');
                this.TranslateExpression(binary.Right);
                this._sql.Append(" IS NULL)");
                return;
            case ExpressionType.NotEqual when IsNull(binary.Left):
                this._sql.Append('(');
                this.TranslateExpression(binary.Right);
                this._sql.Append(" IS NOT NULL)");
                return;
        }

        // For NotEqual with non-null values, generate NULL-safe comparison:
        // (x <> val OR x IS NULL) to match SQL expectation that NULL != val is TRUE
        if (binary.NodeType == ExpressionType.NotEqual && !IsNull(binary.Left) && !IsNull(binary.Right))
        {
            this._sql.Append("((");
            this.TranslateExpression(binary.Left);
            this._sql.Append(" <> ");
            this.TranslateExpression(binary.Right);
            this._sql.Append(") OR (");
            this.TranslateExpression(binary.Left);
            this._sql.Append(" IS NULL) OR (");
            this.TranslateExpression(binary.Right);
            this._sql.Append(" IS NULL))");
            return;
        }

        this._sql.Append('(');
        this.TranslateExpression(binary.Left, isSearchCondition: binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse);

        this._sql.Append(binary.NodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " <> ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.AndAlso => " AND ",
            ExpressionType.OrElse => " OR ",
            _ => throw new NotSupportedException("Unsupported binary expression node type: " + binary.NodeType)
        });

        this.TranslateExpression(binary.Right, isSearchCondition: binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse);
        this._sql.Append(')');

        static bool IsNull(Expression expression)
            => expression is ConstantExpression { Value: null };
    }

    private void TranslateConstant(object? value, bool isSearchCondition)
    {
        switch (value)
        {
            // NULL is a keyword, not a value — safe to inline.
            case null:
                this._sql.Append("NULL");
                return;

            // Top-level boolean expressions need to expand to a comparison so ClickHouse treats
            // them as a search condition; a parameter would come back as the literal "true"/"false"
            // instead of a predicate.
            case bool b when isSearchCondition:
                this._sql.Append(b ? "1 = 1" : "1 = 0");
                return;

            default:
                this._sql.Append(this._parameters.AppendValue(value, ClickHouseTypeMap.GetColumnType(value.GetType())));
                return;
        }
    }

    private void TranslateMember(MemberExpression member, bool isSearchCondition)
    {
        if (this.TryBindProperty(member, out var property))
        {
            this.GenerateColumn(property, isSearchCondition);
            return;
        }

        throw new NotSupportedException($"Member access for '{member.Member.Name}' is unsupported - only member access over the filter parameter are supported");
    }

    private void TranslateMethodCall(MethodCallExpression methodCall, bool isSearchCondition)
    {
        if (this.TryBindProperty(methodCall, out var property))
        {
            this.GenerateColumn(property, isSearchCondition);
            return;
        }

        switch (methodCall)
        {
            // Enumerable.Contains()
            case { Method.Name: nameof(Enumerable.Contains), Arguments: [var source, var item] } contains
                when contains.Method.DeclaringType == typeof(Enumerable):
                this.TranslateContains(source, item);
                return;

            // List<T>.Contains()
            case { Method: { Name: nameof(Enumerable.Contains), DeclaringType: { IsGenericType: true } declaringType }, Object: { } source, Arguments: [var item] }
                when declaringType.GetGenericTypeDefinition() == typeof(List<>):
                this.TranslateContains(source, item);
                return;

            // MemoryExtensions.Contains() — used for Span/ReadOnlySpan/array Contains
            case { Method: { Name: "Contains", DeclaringType.Name: "MemoryExtensions" }, Arguments: [var memSource, var memItem] }:
                this.TranslateContains(memSource, memItem);
                return;

            // Enumerable.Any() with predicate
            case { Method.Name: nameof(Enumerable.Any), Arguments: [var anySource, LambdaExpression lambda] } any
                when any.Method.DeclaringType == typeof(Enumerable):
                this.TranslateAny(anySource, lambda);
                return;

            default:
                throw new NotSupportedException($"Unsupported method call: {methodCall.Method.DeclaringType?.Name}.{methodCall.Method.Name}");
        }
    }

    private void TranslateContains(Expression source, Expression item)
    {
        // Unwrap implicit conversions (e.g. MemoryExtensions.Contains wraps arrays in Convert)
        var unwrappedSource = source;
        while (unwrappedSource is UnaryExpression { NodeType: ExpressionType.Convert } convert)
        {
            unwrappedSource = convert.Operand;
        }

        // Check property binding on unwrapped source (or recursively inside method calls for span wrappers)
        if (this.TryBindPropertyRecursive(unwrappedSource, out var boundProperty))
        {
            // Contains over array column: r.Strings.Contains("foo") → has(`Strings`, 'foo')
            this._sql.Append("has(");
            this.GenerateColumn(boundProperty);
            this._sql.Append(", ");
            this.TranslateExpression(item);
            this._sql.Append(')');
            return;
        }

        switch (unwrappedSource)
        {
            // Contains over inline array: new[] { "a", "b" }.Contains(r.String) → `String` IN ('a', 'b')
            case NewArrayExpression newArray:
                this.TranslateExpression(item);
                this._sql.Append(" IN (");
                for (int i = 0; i < newArray.Expressions.Count; i++)
                {
                    if (i > 0) this._sql.Append(", ");
                    this.TranslateExpression(newArray.Expressions[i]);
                }
                this._sql.Append(')');
                return;

            // Contains over captured array: capturedArray.Contains(r.String) → `String` IN ('a', 'b')
            case ConstantExpression { Value: IEnumerable elements and not string }:
                this.TranslateExpression(item);
                this._sql.Append(" IN (");
                bool first = true;
                foreach (var element in elements)
                {
                    if (!first) this._sql.Append(", ");
                    first = false;
                    this.TranslateConstant(element, isSearchCondition: false);
                }
                this._sql.Append(')');
                return;

            // Fallback: for MemoryExtensions.Contains, the source may be a method call converting
            // an array to ReadOnlySpan. Try to find a NewArrayExpression or ConstantExpression inside.
            default:
                if (TryExtractArrayElements(unwrappedSource, out var extractedElements))
                {
                    this.TranslateExpression(item);
                    this._sql.Append(" IN (");
                    bool evalFirst = true;
                    foreach (var element in extractedElements)
                    {
                        if (!evalFirst) this._sql.Append(", ");
                        evalFirst = false;
                        this.TranslateConstant(element, isSearchCondition: false);
                    }
                    this._sql.Append(')');
                    return;
                }
                throw new NotSupportedException("Unsupported Contains expression");
        }
    }

    private void TranslateAny(Expression source, LambdaExpression lambda)
    {
        if (!this.TryBindProperty(source, out var property)
            || lambda.Body is not MethodCallExpression containsCall)
        {
            throw new NotSupportedException("Unsupported method call: Enumerable.Any");
        }

        // We support: r.ArrayCol.Any(x => values.Contains(x)) → hasAny(`ArrayCol`, ['a', 'b'])
        // Extract the values from the Contains call
        IEnumerable? values = null;
        if (containsCall.Method.Name == nameof(Enumerable.Contains))
        {
            Expression? valuesExpr = containsCall.Method.DeclaringType == typeof(Enumerable)
                ? containsCall.Arguments[0]
                : containsCall.Object;

            if (valuesExpr is ConstantExpression { Value: IEnumerable enumerable })
            {
                values = enumerable;
            }
            else if (valuesExpr is NewArrayExpression newArray)
            {
                var list = new List<object?>();
                foreach (var expr in newArray.Expressions)
                {
                    if (expr is ConstantExpression { Value: var v })
                        list.Add(v);
                    else
                        throw new NotSupportedException("Unsupported method call: Enumerable.Any");
                }
                values = list;
            }
        }

        if (values is null)
        {
            throw new NotSupportedException("Unsupported method call: Enumerable.Any");
        }

        this._sql.Append("hasAny(");
        this.GenerateColumn(property);
        this._sql.Append(", [");
        bool first = true;
        foreach (var element in values)
        {
            if (!first) this._sql.Append(", ");
            first = false;
            this.TranslateConstant(element, isSearchCondition: false);
        }
        this._sql.Append("])");
    }

    private void TranslateUnary(UnaryExpression unary, bool isSearchCondition)
    {
        switch (unary.NodeType)
        {
            case ExpressionType.Not:
                if (unary.Operand is BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } binary)
                {
                    this.TranslateBinary(
                        Expression.MakeBinary(
                            binary.NodeType is ExpressionType.Equal ? ExpressionType.NotEqual : ExpressionType.Equal,
                            binary.Left,
                            binary.Right));
                    return;
                }

                // Use ifNull to handle NULL → TRUE for NOT expressions in ClickHouse
                this._sql.Append("(NOT ifNull(");
                this.TranslateExpression(unary.Operand, isSearchCondition);
                this._sql.Append(", 0))");
                return;

            // Handle Convert for nullable → non-nullable
            case ExpressionType.Convert when Nullable.GetUnderlyingType(unary.Type) == unary.Operand.Type:
                this.TranslateExpression(unary.Operand, isSearchCondition);
                return;

            // Handle Convert for dynamic dictionary access
            case ExpressionType.Convert when this.TryBindProperty(unary.Operand, out var property) && unary.Type == property.Type:
                this.GenerateColumn(property, isSearchCondition);
                return;

            default:
                throw new NotSupportedException("Unsupported unary expression node type: " + unary.NodeType);
        }
    }

    private void GenerateColumn(PropertyModel property, bool isSearchCondition = false)
    {
        this._sql.Append('`').Append(property.StorageName.Replace("`", "``", StringComparison.Ordinal)).Append('`');

        if (isSearchCondition)
        {
            this._sql.Append(" = 1");
        }
    }

    private bool TryBindProperty(Expression expression, [NotNullWhen(true)] out PropertyModel? property)
    {
        var unwrappedExpression = expression;
        while (unwrappedExpression is UnaryExpression { NodeType: ExpressionType.Convert } convert)
        {
            unwrappedExpression = convert.Operand;
        }

        var modelName = unwrappedExpression switch
        {
            MemberExpression memberExpression when memberExpression.Expression == this._recordParameter
                => memberExpression.Member.Name,

            MethodCallExpression
            {
                Method: { Name: "get_Item", DeclaringType: var declaringType },
                Arguments: [ConstantExpression { Value: string keyName }]
            } methodCall when methodCall.Object == this._recordParameter && declaringType == typeof(Dictionary<string, object?>)
                => keyName,

            _ => null
        };

        if (modelName is null)
        {
            property = null;
            return false;
        }

        if (!this._model.PropertyMap.TryGetValue(modelName, out property))
        {
            throw new InvalidOperationException($"Property name '{modelName}' provided as part of the filter clause is not a valid property name.");
        }

        return true;
    }

    /// <summary>
    /// Tries to bind a property from an expression, including through Convert and MethodCall wrappers
    /// (e.g. MemoryExtensions.Contains wraps array fields in span conversion calls).
    /// </summary>
    private bool TryBindPropertyRecursive(Expression expression, [NotNullWhen(true)] out PropertyModel? property)
    {
        if (this.TryBindProperty(expression, out property))
            return true;

        // Unwrap conversions
        if (expression is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return TryBindPropertyRecursive(convert.Operand, out property);

        // Check inside method call arguments (span conversion wrappers)
        if (expression is MethodCallExpression methodCall)
        {
            foreach (var arg in methodCall.Arguments)
            {
                if (TryBindPropertyRecursive(arg, out property))
                    return true;
            }
            if (methodCall.Object is not null && TryBindPropertyRecursive(methodCall.Object, out property))
                return true;
        }

        property = null;
        return false;
    }

    /// <summary>
    /// Recursively searches through an expression tree to find array values,
    /// handling MemoryExtensions.Contains patterns where arrays are wrapped in span conversions.
    /// </summary>
    private static bool TryExtractArrayElements(Expression expression, [NotNullWhen(true)] out List<object?>? elements)
    {
        // Unwrap conversions
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } convert)
        {
            expression = convert.Operand;
        }

        if (expression is NewArrayExpression newArray)
        {
            elements = new List<object?>(newArray.Expressions.Count);
            foreach (var expr in newArray.Expressions)
            {
                if (expr is ConstantExpression { Value: var v })
                    elements.Add(v);
                else
                {
                    elements = null;
                    return false;
                }
            }
            return true;
        }

        if (expression is ConstantExpression { Value: IEnumerable enumerable and not string })
        {
            elements = [];
            foreach (var item in enumerable)
                elements.Add(item);
            return true;
        }

        // For method calls like RuntimeHelpers.CreateSpan or similar, check the first argument
        if (expression is MethodCallExpression methodCall)
        {
            foreach (var arg in methodCall.Arguments)
            {
                if (TryExtractArrayElements(arg, out elements))
                    return true;
            }
            if (methodCall.Object is not null && TryExtractArrayElements(methodCall.Object, out elements))
                return true;
        }

        elements = null;
        return false;
    }

}
