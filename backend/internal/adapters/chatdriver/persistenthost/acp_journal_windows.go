//go:build windows

package persistenthost

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openACPJournalFile(path string) (*os.File, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	// Supply the protected DACL at creation so even an empty new journal never
	// inherits broad access. Existing journals are secured by handle below.
	sid := user.User.Sid.String()
	sd, err := windows.SecurityDescriptorFromString("O:" + sid + "D:P(A;;FA;;;" + sid + ")(A;;FA;;;SY)(A;;FA;;;BA)")
	if err != nil {
		return nil, err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	attributes := windows.SecurityAttributes{
		Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd,
	}
	handle, err := windows.CreateFile(name,
		windows.GENERIC_READ|windows.GENERIC_WRITE|windows.READ_CONTROL|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, &attributes, windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	secured := false
	defer func() {
		if !secured {
			_ = file.Close()
		}
	}()
	actual, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return nil, err
	}
	owner, _, err := actual.Owner()
	if err != nil {
		return nil, err
	}
	if owner == nil || !owner.Equals(user.User.Sid) {
		return nil, fmt.Errorf("secure persistent ACP prompt journal: owner is not current user")
	}
	if err := windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil); err != nil {
		return nil, fmt.Errorf("secure persistent ACP prompt journal: %w", err)
	}
	// OPEN_ALWAYS preserves an existing file until its confidentiality is
	// established. Keep truncation on the same secured handle.
	if err := file.Truncate(0); err != nil {
		return nil, err
	}
	secured = true
	return file, nil
}
