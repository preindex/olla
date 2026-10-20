package domain

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// MaxParameterBillions caps configured ranges and parsed metadata at one billion
// billion parameters, well beyond current model sizes while keeping arithmetic finite.
const MaxParameterBillions = 1e9

var parameterSizePattern = regexp.MustCompile(`(?i)^([0-9]+(?:\.[0-9]+)?)\s*([bm])$`)

// ParameterRange defines inclusive model parameter-count bounds in billions.
type ParameterRange struct {
	MinB *float64 `yaml:"min_b,omitempty" json:"min_b,omitempty"`
	MaxB *float64 `yaml:"max_b,omitempty" json:"max_b,omitempty"`
}

// Clone creates an independent copy of the range bounds.
func (r *ParameterRange) Clone() *ParameterRange {
	if r == nil {
		return nil
	}
	return &ParameterRange{MinB: cloneFloat(r.MinB), MaxB: cloneFloat(r.MaxB)}
}

// ValidateParameterRange checks configured inclusive parameter-count bounds.
func (fc *FilterConfig) ValidateParameterRange() error {
	if fc == nil {
		return nil
	}
	if fc.ParameterRange == nil {
		return nil
	}
	r := fc.ParameterRange
	for name, bound := range map[string]*float64{"min_b": r.MinB, "max_b": r.MaxB} {
		if bound == nil {
			continue
		}
		if math.IsNaN(*bound) || math.IsInf(*bound, 0) || *bound < 0 || *bound > MaxParameterBillions {
			return fmt.Errorf("%s must be finite and between 0 and %g", name, MaxParameterBillions)
		}
	}
	if r.MinB != nil && r.MaxB != nil && *r.MinB > *r.MaxB {
		return errors.New("min_b must be less than or equal to max_b")
	}
	return nil
}

// ParameterBillions parses structured parameter-size metadata such as "7B" or
// "22000M". Unrecognised or ambiguous values are deliberately rejected.
func ParameterBillions(size string) (float64, bool) {
	match := parameterSizePattern.FindStringSubmatch(strings.TrimSpace(size))
	if match == nil {
		return 0, false
	}
	n, err := strconv.ParseFloat(match[1], 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, false
	}
	if strings.EqualFold(match[2], "m") {
		n /= 1000
	}
	if n <= 0 || n > MaxParameterBillions {
		return 0, false
	}
	return n, true
}

// MatchesParameterRange reports whether a model has known parameter metadata
// within the configured inclusive range. Missing and malformed values do not match.
func (fc *FilterConfig) MatchesParameterRange(model *ModelInfo) bool {
	if fc == nil || fc.ParameterRange == nil {
		return true
	}
	r := fc.ParameterRange
	if r.MinB == nil && r.MaxB == nil {
		return true
	}
	if model == nil || model.Details == nil || model.Details.ParameterSize == nil {
		return false
	}
	billions, ok := ParameterBillions(*model.Details.ParameterSize)
	if !ok {
		return false
	}
	return (r.MinB == nil || billions >= *r.MinB) && (r.MaxB == nil || billions <= *r.MaxB)
}

func mergeLowerBound(left, right *float64) *float64 {
	if left == nil {
		return cloneFloat(right)
	}
	if right == nil || *left >= *right {
		return cloneFloat(left)
	}
	return cloneFloat(right)
}

func mergeUpperBound(left, right *float64) *float64 {
	if left == nil {
		return cloneFloat(right)
	}
	if right == nil || *left <= *right {
		return cloneFloat(left)
	}
	return cloneFloat(right)
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
