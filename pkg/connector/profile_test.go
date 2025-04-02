package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestCustomFieldConvertToString(t *testing.T) {
	ctx := context.Background()
	data := `{
		"customSchemas": {
		  "employmentData": {
			"employeeNumber": "123456789",
			"jobFamily": "Engineering",
			"location": "Atlanta",
			"jobLevel": 8,
			"projects": [
			  { "value": "GeneGnome" },
			  { "value": "Panopticon", "type": "work" },
			  { "value": "MegaGene", "type": "custom", "customType": "secret" }
			]
		  }
		}
	  }`
	v := struct {
		CustomSchemas map[string]googleapi.RawMessage `json:"customSchemas"`
	}{}
	err := json.Unmarshal([]byte(data), &v)
	require.NoError(t, err)

	profile := flattenCustomSchemas(ctx, v.CustomSchemas)
	require.Equal(t, map[string]string{
		"employmentdata.employeenumber": "123456789",
		"employmentdata.jobfamily":      "Engineering",
		"employmentdata.location":       "Atlanta",
		"employmentdata.joblevel":       "8",
		"employmentdata.projects":       "GeneGnome, Panopticon, MegaGene",
	}, profile)
}

// TestNormalizeName evaluates the normalizeName function across various scenarios.
func TestNormalizeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Spaces replaced with underscore",
			input:    "Example Name",
			expected: "example_name",
		},
		{
			name:     "Symbol replaced with underscore",
			input:    "Example@Name",
			expected: "example_name",
		},
		{
			name:     "Multiple consecutive non-alphanumeric characters collapsed",
			input:    "Multiple___Underscores___Here",
			expected: "multiple_underscores_here",
		},
		{
			name:     "Leading and trailing non-alphanumeric characters removed",
			input:    "__Leading_and_Trailing_Underscores__",
			expected: "leading_and_trailing_underscores",
		},
		{
			name:     "Special characters replaced with underscore",
			input:    "SpecialChars*&^%$#@!",
			expected: "specialchars",
		},
		{
			name:     "Mixed alphanumeric content remains unchanged",
			input:    "Mixed123Content456",
			expected: "mixed123content456",
		},
		{
			name:     "Empty string remains empty",
			input:    "",
			expected: "",
		},
		{
			name:     "String with only non-alphanumeric characters becomes empty",
			input:    "!@#$%^&*()",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeName(tt.input)
			require.Equal(t, tt.expected, result, "Input: %s", tt.input)
		})
	}
}
