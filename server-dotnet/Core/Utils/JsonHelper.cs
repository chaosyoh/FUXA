using Core.Models;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Core.Utils
{
    public static class JsonHelper
    {
        /// <summary>
        /// Default deserialization options: case-insensitive, number-to-string tolerance.
        /// </summary>
        public static JsonSerializerOptions Default { get; } = CreateDefault();

        public static JsonSerializerOptions CamelCase { get; } = CreateDefault(o =>
        {
            o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        });

        public static JsonSerializerOptions IgnoreNull { get; } = CreateDefault(o =>
        {
            o.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
        });

        private static JsonSerializerOptions CreateDefault(Action<JsonSerializerOptions>? configure = null)
        {
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
            };
            options.Converters.Add(new FlexibleStringConverter());
            configure?.Invoke(options);
            return options;
        }

        public static T? Copy<T>(this object value)
        {
            string json = value is JsonElement je
                ? je.GetRawText()
                : JsonSerializer.Serialize(value);
            return JsonSerializer.Deserialize<T>(json, Default);
        }

        /// <summary>
        /// Serialize object to JSON string, handling JsonElement correctly.
        /// </summary>
        public static string Serialize(object? value, JsonSerializerOptions? options = null)
        {
            if (value is JsonElement je)
                return je.GetRawText();
            return JsonSerializer.Serialize(value, options ?? Default);
        }
    }

    /// <summary>
    /// Allows deserializing JSON number/boolean tokens into string properties.
    /// Newtonsoft.Json did this automatically; System.Text.Json requires explicit handling.
    /// </summary>
    public class FlexibleStringConverter : JsonConverter<string>
    {
        public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.Number => reader.TryGetInt64(out var l) ? l.ToString() : reader.GetDouble().ToString(),
                JsonTokenType.True => "true",
                JsonTokenType.False => "false",
                JsonTokenType.Null => null,
                _ => throw new JsonException($"Unexpected token type {reader.TokenType} for string property")
            };
        }

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }

    /// <summary>
    /// Converter for Scale?: when JSON value is empty string "", deserialize as null.
    /// Node.js frontend stores "" for unset Scale objects.
    /// </summary>
    public class ScaleConverter : JsonConverter<Scale?>
    {
        public override Scale? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType == JsonTokenType.String)
            {
                // empty string → null
                reader.GetString();
                return null;
            }

            return JsonSerializer.Deserialize<Scale>(ref reader);
        }

        public override void Write(Utf8JsonWriter writer, Scale? value, JsonSerializerOptions options)
        {
            if (value == null)
                writer.WriteNullValue();
            else
                JsonSerializer.Serialize(writer, value, options);
        }
    }

    /// <summary>
    /// Converter for TagDeadband?: when JSON value is empty string "", deserialize as null.
    /// </summary>
    public class TagDeadbandConverter : JsonConverter<TagDeadband?>
    {
        public override TagDeadband? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType == JsonTokenType.String)
            {
                reader.GetString();
                return null;
            }

            return JsonSerializer.Deserialize<TagDeadband>(ref reader);
        }

        public override void Write(Utf8JsonWriter writer, TagDeadband? value, JsonSerializerOptions options)
        {
            if (value == null)
                writer.WriteNullValue();
            else
                JsonSerializer.Serialize(writer, value, options);
        }
    }
}
