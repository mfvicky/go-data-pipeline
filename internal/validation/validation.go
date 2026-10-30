package validation

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type Validator struct {
	requiredFields []string
	fieldTypes     map[string]string
	minValues      map[string]float64
	maxValues      map[string]float64
}

func New(rules map[string]any) (*Validator, error) {
	v := &Validator{
		fieldTypes: make(map[string]string),
		minValues:  make(map[string]float64),
		maxValues:  make(map[string]float64),
	}

	if value, exists := rules["required_fields"]; exists {
		var fields []string
		switch list := value.(type) {
		case []string:
			fields = list
		case []any:
			for _, item := range list {
				field, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("required_fields must contain strings")
				}
				fields = append(fields, field)
			}
		default:
			return nil, fmt.Errorf("required_fields must be an array")
		}
		for _, field := range fields {
			if strings.TrimSpace(field) == "" {
				return nil, fmt.Errorf("required_fields cannot contain empty names")
			}
			v.requiredFields = append(v.requiredFields, field)
		}
	}

	if value, exists := rules["field_types"]; exists {
		types, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field_types must be an object")
		}
		for field, value := range types {
			kind, ok := value.(string)
			if !ok || strings.TrimSpace(field) == "" {
				return nil, fmt.Errorf("field_types must contain valid field names and types")
			}
			switch kind {
			case "string", "number", "int", "bool":
				v.fieldTypes[field] = kind
			default:
				return nil, fmt.Errorf("unsupported field type %q", kind)
			}
		}
	}

	var err error
	v.minValues, err = readLimits(rules, "min_values")
	if err != nil {
		return nil, err
	}
	v.maxValues, err = readLimits(rules, "max_values")
	if err != nil {
		return nil, err
	}
	for field, min := range v.minValues {
		if max, exists := v.maxValues[field]; exists && min > max {
			return nil, fmt.Errorf("min_values.%s exceeds max_values.%s", field, field)
		}
	}

	return v, nil
}

func (v *Validator) Check(record domain.Record) error {
	for _, field := range v.requiredFields {
		value, exists := record.Data[field]
		if !exists || value == nil {
			return fmt.Errorf("required field %q is missing", field)
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			return fmt.Errorf("required field %q is blank", field)
		}
	}

	for field, kind := range v.fieldTypes {
		value, exists := record.Data[field]
		if !exists || value == nil {
			continue
		}
		switch kind {
		case "string":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("field %q must be a string", field)
			}
		case "bool":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("field %q must be a bool", field)
			}
		case "number", "int":
			number, err := numericValue(value, false)
			if err != nil || (kind == "int" && number != math.Trunc(number)) {
				return fmt.Errorf("field %q must be %s", field, kind)
			}
		}
	}

	for field, min := range v.minValues {
		if value, exists := record.Data[field]; exists && value != nil {
			number, err := numericValue(value, true)
			if err != nil || number < min {
				return fmt.Errorf("field %q must be >= %v", field, min)
			}
		}
	}
	for field, max := range v.maxValues {
		if value, exists := record.Data[field]; exists && value != nil {
			number, err := numericValue(value, true)
			if err != nil || number > max {
				return fmt.Errorf("field %q must be <= %v", field, max)
			}
		}
	}
	return nil
}

func (v *Validator) Filter(jobID string, records []domain.Record) ([]domain.Record, []domain.PipelineError) {
	valid := make([]domain.Record, 0, len(records))
	failures := make([]domain.PipelineError, 0)

	for _, record := range records {
		if err := v.Check(record); err != nil {
			failures = append(failures, domain.PipelineError{
				JobID:     jobID,
				Stage:     "validation",
				RecordID:  record.ID,
				Message:   err.Error(),
				Timestamp: time.Now().UTC(),
			})
			continue
		}
		valid = append(valid, record)
	}
	return valid, failures
}

func readLimits(rules map[string]any, key string) (map[string]float64, error) {
	limits := make(map[string]float64)
	value, exists := rules[key]
	if !exists {
		return limits, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", key)
	}
	for field, raw := range fields {
		number, err := numericValue(raw, true)
		if strings.TrimSpace(field) == "" || err != nil {
			return nil, fmt.Errorf("%s.%s must be a number", key, field)
		}
		limits[field] = number
	}
	return limits, nil
}

func numericValue(value any, allowString bool) (float64, error) {
	var number float64
	switch v := value.(type) {
	case float64:
		number = v
	case float32:
		number = float64(v)
	case int:
		number = float64(v)
	case int64:
		number = float64(v)
	case json.Number:
		parsed, err := v.Float64()
		if err != nil {
			return 0, err
		}
		number = parsed
	case string:
		if !allowString {
			return 0, fmt.Errorf("expected a numeric value")
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, err
		}
		number = parsed
	default:
		return 0, fmt.Errorf("expected a numeric value")
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, fmt.Errorf("number must be finite")
	}
	return number, nil
}
