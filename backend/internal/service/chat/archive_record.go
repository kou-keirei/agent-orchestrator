package chat

import "reflect"

// dropEmptyArchiveFields omits empty scalar strings and nil pointers/interfaces
// from the normalized archive envelope. Typed decoders read these as zero values
// when absent. Call before adding input and error: their presence is meaningful,
// including an error with an empty message.
//
// Unlike the upstream filter, keep slices/maps and raw JSON intact. An explicit
// empty collection can mean a reset, and a raw JSON decoder can distinguish null
// from an absent field. Do not recursively compact provider payloads or discard
// false, zero, or a non-nil pointer to a zero value.
func dropEmptyArchiveFields(record map[string]any) {
	for key, value := range record {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.Invalid:
			delete(record, key)
		case reflect.String:
			if v.Len() == 0 {
				delete(record, key)
			}
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				delete(record, key)
			}
		}
	}
}
