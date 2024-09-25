package timeline

import (
	"fmt"

	"encoding/json"
)

func CreateConvertorToString[Val any]() func(Val) string {
	switch interface{}(new(Val)).(type) {
	case *string:
		return func(v Val) string { return interface{}(v).(string) }
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *bool, *float32, *float64:
		return func(v Val) string { return fmt.Sprintf("%v", v) }
	default:
		isStringer := typeImplementsInterface[Val, fmt.Stringer]()
		if isStringer {
			return func(v Val) string { return interface{}(v).(fmt.Stringer).String() }
		}

		return func(v Val) string {
			jv, err := json.Marshal(v)
			if err != nil {
				panic(fmt.Sprintf("unable to marshal value of type %T: %v", v, err))
			}
			return string(jv)
		}
	}
}
