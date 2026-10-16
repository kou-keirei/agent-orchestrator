package main

import (
	"encoding/json"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"reflect"
	"strings"
	"testing"
)

func TestEnumIdentifiersPreserveWireValues(t *testing.T) {
	for _, kind := range []string{"named", "enumUnion", "taggedUnion"} {
		for _, tc := range []struct {
			name   string
			values []string
			want   map[string]string
		}{
			{
				name:   "pinned collision",
				values: []string{"form", "openai/form", "openaiForm", "url"},
				want:   map[string]string{"form": "Form", "openai/form": "OpenaiForm", "openaiForm": "OpenaiForm2", "url": "URL"},
			},
			{
				name:   "suffix cannot steal existing name",
				values: []string{"foo/bar", "fooBar", "fooBar2", "fooBar3"},
				want:   map[string]string{"foo/bar": "FooBar", "fooBar": "FooBar4", "fooBar2": "FooBar2", "fooBar3": "FooBar3"},
			},
			{
				name:   "unchanged distinct names",
				values: []string{"completed", "inProgress", "thread/read", "threadId"},
				want:   map[string]string{"completed": "Completed", "inProgress": "InProgress", "thread/read": "ThreadRead", "threadId": "ThreadID"},
			},
			{
				name:   "duplicate raw values",
				values: []string{"form", "form", "url"},
				want:   map[string]string{"form": "Form", "url": "URL"},
			},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				prefix := "Collision"
				if kind == "taggedUnion" {
					prefix += "Type"
				}
				want := make(map[string]string, len(tc.want))
				for wire, suffix := range tc.want {
					want[wire] = prefix + suffix
				}
				first := renderEnumFixture(kind, tc.values)
				for i := 0; i < 5; i++ {
					if again := renderEnumFixture(kind, tc.values); again != first {
						t.Fatal("same schema produced nondeterministic source")
					}
				}
				if got := compiledEnumValues(t, first); !reflect.DeepEqual(got, want) {
					t.Fatalf("wire values / names = %v, want %v\n%s", got, want, first)
				}
				// Changing schema order must not reassign a wire value's Go name.
				reversed := append([]string(nil), tc.values...)
				for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
					reversed[i], reversed[j] = reversed[j], reversed[i]
				}
				if got := compiledEnumValues(t, renderEnumFixture(kind, reversed)); !reflect.DeepEqual(got, want) {
					t.Fatalf("schema order changed wire names: %v, want %v", got, want)
				}
			})
		}
	}
}

func renderEnumFixture(kind string, values []string) string {
	s := &schema{}
	for _, value := range values {
		arm := &schema{Type: json.RawMessage(`"string"`), Enum: []any{value}}
		switch kind {
		case "named":
			s.Type = json.RawMessage(`"string"`)
			s.Enum = append(s.Enum, value)
		case "enumUnion":
			s.OneOf = append(s.OneOf, arm)
		case "taggedUnion":
			s.OneOf = append(s.OneOf, &schema{
				Properties: map[string]*schema{"mode": arm}, Required: []string{"mode"},
			})
		}
	}
	var out strings.Builder
	out.WriteString("package fixture\n")
	g := &generator{defs: map[string]*schema{}}
	var inline []inlineType
	g.renderDef(&out, "Collision", s, &inline, map[string]bool{})
	return out.String()
}

// Type-check the generated Go and inspect its actual constant values, so a name
// collision or a missing wire variant cannot hide behind a string assertion.
func compiledEnumValues(t *testing.T, src string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatalf("parse generated fixture: %v\n%s", err, src)
	}
	pkg, err := new(types.Config).Check("fixture", fset, []*ast.File{f}, nil)
	if err != nil {
		t.Fatalf("type-check generated fixture: %v\n%s", err, src)
	}
	values := map[string]string{}
	for _, name := range pkg.Scope().Names() {
		if c, ok := pkg.Scope().Lookup(name).(*types.Const); ok {
			wire := constant.StringVal(c.Val())
			if _, duplicate := values[wire]; duplicate {
				t.Fatalf("wire value %q has multiple constants", wire)
			}
			values[wire] = name
		}
	}
	return values
}
