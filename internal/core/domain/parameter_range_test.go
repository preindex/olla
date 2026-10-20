package domain

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParameterBillions(t *testing.T) {
	tests := []struct {
		input string
		want  float64
		ok    bool
	}{
		{input: "7B", want: 7, ok: true},
		{input: " 14.7b ", want: 14.7, ok: true},
		{input: "22000M", want: 22, ok: true},
		{input: "8", ok: false},
		{input: "qwen-7b", ok: false},
		{input: "7B Q4", ok: false},
		{input: "NaN", ok: false},
		{input: "1e999B", ok: false},
		{input: "1000000000000B", ok: false},
		{input: "0B", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParameterBillions(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.InDelta(t, tt.want, got, 1e-9)
			}
		})
	}
}

func TestFilterConfigParameterRange(t *testing.T) {
	min, max := 7.0, 13.0
	config := &FilterConfig{ParameterRange: &ParameterRange{MinB: &min, MaxB: &max}}
	require.NoError(t, config.Validate())
	for _, tc := range []struct {
		model *ModelInfo
		want  bool
	}{
		{model: &ModelInfo{Details: &ModelDetails{ParameterSize: stringPointer("7B")}}, want: true},
		{model: &ModelInfo{Details: &ModelDetails{ParameterSize: stringPointer("13b")}}, want: true},
		{model: &ModelInfo{Details: &ModelDetails{ParameterSize: stringPointer("14B")}}, want: false},
		{model: &ModelInfo{Details: &ModelDetails{ParameterSize: stringPointer("unknown")}}, want: false},
		{model: &ModelInfo{Name: "llama-7b"}, want: false},
		{model: &ModelInfo{Size: 7_000_000_000}, want: false},
	} {
		assert.Equal(t, tc.want, config.MatchesParameterRange(tc.model))
	}
}

func TestFilterConfigParameterRangeValidationAndOperations(t *testing.T) {
	negative, tooLarge, min, max := -1.0, MaxParameterBillions+1, 10.0, 5.0
	for _, invalid := range []*FilterConfig{
		{ParameterRange: &ParameterRange{MinB: &negative}},
		{ParameterRange: &ParameterRange{MaxB: &tooLarge}},
		{ParameterRange: &ParameterRange{MinB: &min, MaxB: &max}},
		{ParameterRange: &ParameterRange{MinB: floatPointer(math.NaN())}},
		{ParameterRange: &ParameterRange{MaxB: floatPointer(math.Inf(1))}},
	} {
		assert.Error(t, invalid.Validate())
	}

	a, b := 4.0, 9.0
	original := &FilterConfig{Include: []string{"llama*"}, ParameterRange: &ParameterRange{MinB: &a, MaxB: &b}}
	clone := original.Clone()
	require.NotSame(t, original.ParameterRange.MinB, clone.ParameterRange.MinB)
	*clone.ParameterRange.MinB = 5
	assert.Equal(t, 4.0, *original.ParameterRange.MinB)
	assert.False(t, original.IsEmpty())

	c, d := 6.0, 8.0
	merged := original.Merge(&FilterConfig{ParameterRange: &ParameterRange{MinB: &c, MaxB: &d}})
	assert.Equal(t, 6.0, *merged.ParameterRange.MinB)
	assert.Equal(t, 8.0, *merged.ParameterRange.MaxB)
	assert.Equal(t, []string{"llama*"}, merged.Include)
}

func stringPointer(value string) *string  { return &value }
func floatPointer(value float64) *float64 { return &value }
