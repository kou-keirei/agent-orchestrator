package store

// LockReportWriterForTest lets the external delivery tests contend the actual
// writer mutex without adding a production hook or touching another database.
func LockReportWriterForTest(s *Store) func() {
	s.writeMu.Lock()
	return s.writeMu.Unlock
}
