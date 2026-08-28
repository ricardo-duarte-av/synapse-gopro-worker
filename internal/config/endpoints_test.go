package config

import (
	"reflect"
	"testing"
)

// Every field of Endpoints must appear in ByName.
//
// ByName is the single source for both the startup banner and the handler's
// mode lookup, so a field missing from it means the endpoint is configured,
// reported as configured, and silently proxied -- which is exactly what
// happened to get_missing_events. Reflection is used rather than a list
// written here, because a list written here is the same mistake one level up:
// it would need updating by the same person who forgot ByName.
func TestByNameCoversEveryEndpointField(t *testing.T) {
	byName := Endpoints{}.ByName()
	typ := reflect.TypeOf(Endpoints{})

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("yaml")
		if tag == "" {
			t.Errorf("field %s has no yaml tag; ByName cannot key on it", f.Name)
			continue
		}
		if _, ok := byName[tag]; !ok {
			t.Errorf("endpoint %q is a config field but missing from ByName(): "+
				"it would be reported as configured and silently proxied", tag)
		}
	}

	// And nothing in ByName that is not a field, which would mean a key that
	// can never be configured.
	if len(byName) != typ.NumField() {
		t.Errorf("ByName has %d entries for %d fields", len(byName), typ.NumField())
	}
}

// ByName must return what was configured, not zero values. A map built with
// the wrong field would still pass the coverage test above.
func TestByNameReturnsTheConfiguredMode(t *testing.T) {
	typ := reflect.TypeOf(Endpoints{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("yaml")

		// Set only this field to a distinctive mode.
		var eps Endpoints
		v := reflect.ValueOf(&eps).Elem().Field(i)
		v.Set(reflect.ValueOf(Mode{Kind: ModeCanary, CanaryPercent: 37}))

		got := eps.ByName()[tag]
		if got.Kind != ModeCanary || got.CanaryPercent != 37 {
			t.Errorf("ByName()[%q] = %+v after setting field %s; "+
				"the map is keyed to the wrong field", tag, got, f.Name)
		}
	}
}
