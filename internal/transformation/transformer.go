package transformation

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type Transformer struct {
	trimStrings bool
	lowercase   map[string]bool
	uppercase   map[string]bool
	conversions map[string]string
}

func New(rules map[string]any) (*Transformer, error) {
	t := &Transformer{conversions: make(map[string]string)}
	if value, exists := rules["trim_strings"]; exists {
		trim, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("trim_strings must be a boolean")
		}
		t.trimStrings = trim
	}

	var err error
	t.lowercase, err = readFields(rules, "lowercase_fields")
	if err != nil {
		return nil, err
	}

	normalize, err := readFields(rules, "normalize")
	if err != nil {
		return nil, err
	}
	for field := range normalize {
		t.lowercase[field] = true
	}
	t.uppercase, err = readFields(rules, "uppercase_fields")
	if err != nil {
		return nil, err
	}
	for field := range t.lowercase {
		if t.uppercase[field] {
			return nil, fmt.Errorf("field %q cannot be both lowercase and uppercase", field)
		}
	}

	if value, exists := rules["type_conversions"]; exists {
		fields, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("type_conversions must be an object")
		}
		for fields, value := range fields {
			kind, ok := value.(string)
			if !ok || strings.TrimSpace(fields) == "" {
				return nil, fmt.Errorf("type_conversions requrires fields names and type strings")
			}
			switch kind {
			case "number", "int", "bool", "string":
				t.conversions[fields] = kind
			default:
				return nil, fmt.Errorf("unsupported conversion type %q for field %q", kind, fields)
			}
		}
	}
	return t, nil

}

func (t *Transformer) Apply(record domain.Record) (domain.Record, error) {
	result := record
	result.Data = make(map[string]any, len(record.Data))

	for field, value := range record.Data {
		if text, ok := value.(string); ok {
			if t.trimStrings {
				text = strings.TrimSpace(text)
			}
			if t.lowercase[field] {
				text = strings.ToLower(strings.TrimSpace(text))
			}
			if t.uppercase[field] {
				text = strings.ToUpper(strings.TrimSpace(text))
			}
			value = text
		}

		if kind, exists := t.conversions[field]; exists && value != nil {
			converted, err := convert(value, kind)
			if err != nil {
				return domain.Record{}, fmt.Errorf("field %q: %w", field, err)
			}
			value = converted
		}
		result.Data[field] = value
	}
	return result, nil
}

func convert(value any, kind string) (any, error) {
	text := strings.TrimSpace(fmt.Sprint(value))
	switch kind {
	case "string":
		return fmt.Sprint(value), nil
	case "number":
		number, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, fmt.Errorf("cannot covert %q to number", text)
		}
		return number, nil
	case "int":
		number, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int", text)
		}
		return number, nil
	case "bool":
		boolean, err := strconv.ParseBool(text)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to bool", text)
		}
		return boolean, nil
	default:
		return nil, fmt.Errorf("unsupported conversion type %q", kind)

	}

}

func readFields(rules map[string]any, key string) (map[string]bool, error) {
	fields := make(map[string]bool)
	value, exists := rules[key]

	if !exists {
		return fields, nil
	}
	var names []string
	switch list := value.(type) {
	case []string:
		names = list
	case []any:
		for _, item := range list {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s must contain string", key)
			}
			names = append(names, name)
		}
	default:
		return nil, fmt.Errorf("%s must be a array of field names", key)
	}

	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%s cannot contain empty field names", key)
		}
		fields[name] = true
	}
	return fields, nil
}
