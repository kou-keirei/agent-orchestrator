package domain

import "testing"

func TestResolvePermissionModeHonorsReadOnlyFloor(t *testing.T) {
	for _, test := range []struct {
		name      string
		floor     PermissionMode
		requested PermissionMode
		want      PermissionMode
		wantErr   bool
	}{
		{name: "read-only request", floor: PermissionModeReadOnly, requested: PermissionModeReadOnly, want: PermissionModeReadOnly},
		{name: "empty request inherits floor", floor: PermissionModeReadOnly, want: PermissionModeReadOnly},
		{name: "broader request rejected", floor: PermissionModeReadOnly, requested: PermissionModeAuto, wantErr: true},
		{name: "unknown request rejected", floor: PermissionModeReadOnly, requested: PermissionMode("unknown"), wantErr: true},
		{name: "ordinary mode preserved", floor: PermissionModeAuto, requested: PermissionModeAcceptEdits, want: PermissionModeAcceptEdits},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolvePermissionMode(test.floor, test.requested)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("mode = %q, want %q", got, test.want)
			}
		})
	}
}
