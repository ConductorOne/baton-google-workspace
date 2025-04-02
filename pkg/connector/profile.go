package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
)

// normalizeName processes a string by:
// 1. Replacing non-alphanumeric characters with underscores.
// 2. Converting all letters to lowercase.
// 3. Removing leading and trailing underscores.
// 4. Collapsing multiple consecutive underscores into a single one.
func normalizeName(name string) string {
	result := make([]rune, 0, len(name))
	prevUnderscore := false

	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			result = append(result, unicode.ToLower(r))
			prevUnderscore = false
		} else if !prevUnderscore {
			result = append(result, '_')
			prevUnderscore = true
		}
	}

	// Convert result back to string
	normalized := string(result)

	// Trim leading and trailing underscores
	normalized = trimUnderscores(normalized)

	return normalized
}

// trimUnderscores removes leading and trailing underscores from a string.
func trimUnderscores(s string) string {
	start := 0
	end := len(s)

	// Find the first non-underscore character from the start
	for start < end && s[start] == '_' {
		start++
	}

	// Find the first non-underscore character from the end
	for end > start && s[end-1] == '_' {
		end--
	}

	return s[start:end]
}

// flattenCustomSchemas processes the custom schemas and flattens them into the profile map.
func flattenCustomSchemas(ctx context.Context, customSchemas map[string]googleapi.RawMessage) map[string]string {
	l := ctxzap.Extract(ctx)
	output := make(map[string]string)
	for schemaName, rawSchemaData := range customSchemas {
		// Normalize the schema name
		normalizedSchemaName := normalizeName(schemaName)

		var schemaData map[string]any
		err := json.Unmarshal(rawSchemaData, &schemaData)
		if err != nil {
			l.Warn("Error unmarshalling custom schema data, skipping schema",
				zap.Error(err),
				zap.String("schema_name", schemaName),
			)
			continue
		}
		for fieldName, fieldValue := range schemaData {
			// Normalize the field name
			normalizedFieldName := normalizeName(fieldName)
			// Create the composite key
			compositeKey := fmt.Sprintf("%s.%s", normalizedSchemaName, normalizedFieldName)
			// Convert the field value to a string representation
			valueStr, err := customFieldConvertToString(ctx, fieldValue)
			if err != nil {
				// This error should ideally not happen with the current fallback logic in customFieldConvertToString,
				// but we keep the check for robustness.
				l.Warn("Skipping field due to unexpected error converting value to string",
					zap.Error(err),
					zap.String("schema_name", normalizedSchemaName),
					zap.String("field_name", normalizedFieldName),
				)
				continue // Skip adding this field to the output map
			}
			// Only add if conversion was successful (valueStr might be an empty string from fallback)
			output[compositeKey] = valueStr
		}
	}
	return output
}

func customFieldConvertToString(ctx context.Context, value any) (string, error) {
	l := ctxzap.Extract(ctx)
	switch v := value.(type) {
	case string:
		return v, nil
	case []string:
		return strings.Join(v, ", "), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case []any:
		// Check if it's the specific multi-value structure: [{"value": ...}, ...]
		if len(v) > 0 {
			if firstElemMap, ok := v[0].(map[string]any); ok {
				if _, valueKeyExists := firstElemMap["value"]; valueKeyExists {
					// Assume it's the multi-value structure
					var strValues []string
					for i, elem := range v {
						elemMap, ok := elem.(map[string]any)
						if !ok {
							l.Warn("Skipping element in multi-value array: expected map[string]any",
								zap.Int("index", i),
								zap.Any("element_type", fmt.Sprintf("%T", elem)),
							)
							continue
						}
						val, ok := elemMap["value"]
						if !ok {
							l.Warn("Skipping element in multi-value array: map does not contain 'value' key",
								zap.Int("index", i),
							)
							continue
						}

						var elemStr string
						if strVal, ok := val.(string); ok {
							// If the value is already a string, use it directly
							elemStr = strVal
						} else {
							// Otherwise, attempt to JSON marshal the value
							jsonBytes, err := json.Marshal(val)
							if err != nil {
								l.Warn("Skipping element in multi-value array: could not marshal value to JSON",
									zap.Error(err),
									zap.Int("index", i),
									zap.Any("value_type", fmt.Sprintf("%T", val)),
								)
								continue // Skip this element on marshal failure
							}
							elemStr = string(jsonBytes)
						}

						strValues = append(strValues, elemStr)
					}
					return strings.Join(strValues, ", "), nil // Return joined string, nil error
				}
			}
		}
		// If not the specific structure, or if empty, fallback to JSON marshaling the whole array
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			l.Warn("Could not marshal []any to JSON, returning empty string", zap.Error(err))
			return "", nil // Return empty string, nil error on marshal failure
		}
		return string(jsonBytes), nil

	case map[string]any:
		// Fallback to JSON marshaling for unexpected maps
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			l.Warn("Could not marshal map[string]any to JSON, returning empty string", zap.Error(err))
			return "", nil // Return empty string, nil error on marshal failure
		}
		return string(jsonBytes), nil

	default:
		// Fallback to JSON marshaling for any other unknown types
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			l.Warn("Could not marshal value to JSON, returning empty string",
				zap.Error(err),
				zap.Any("value_type", fmt.Sprintf("%T", value)),
			)
			return "", nil // Return empty string, nil error on marshal failure
		}
		return string(jsonBytes), nil
	}
}
