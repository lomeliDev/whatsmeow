// Copyright (c) 2026 wzapi
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
	"testing"
	"time"

	"go.mau.fi/util/dbutil"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// wzapi patch 10 regression test.
//
// PutManyLIDMappings must NOT hold lidCacheLock across its DB insert: a history
// sync batch is thousands of round trips, and holding the lock the whole time
// stalls PutLIDMapping (StoreLIDPNMapping) on the incoming-message path
// (whatsmeow #1004 / #1196). This test parks the batch inside the DB Exec and
// checks the cache lock is acquirable while it is parked. Before the patch the
// Lock()+defer Unlock() spanned the whole transaction, so TryLock would fail.

// blockingDriver is a no-op SQL driver whose Exec calls a hook so a test can
// block the batch insert deterministically without a real database.
type blockingDriver struct{ hook func() }

func (d *blockingDriver) Open(string) (driver.Conn, error) { return &blockingConn{d}, nil }

type blockingConn struct{ d *blockingDriver }

func (c *blockingConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *blockingConn) Close() error                        { return nil }
func (c *blockingConn) Begin() (driver.Tx, error)           { return blockingTx{}, nil }

func (c *blockingConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if c.d.hook != nil {
		c.d.hook()
	}
	return driver.RowsAffected(0), nil
}

func (c *blockingConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &emptyRows{}, nil
}

type blockingTx struct{}

func (blockingTx) Commit() error   { return nil }
func (blockingTx) Rollback() error { return nil }

type emptyRows struct{}

func (*emptyRows) Columns() []string         { return nil }
func (*emptyRows) Close() error              { return nil }
func (*emptyRows) Next([]driver.Value) error { return io.EOF }

var registeredBlockingDriver = &blockingDriver{}

func init() {
	sql.Register("wzapi-blocking-lidmap", registeredBlockingDriver)
}

func TestPutManyLIDMappingsDoesNotHoldCacheLockDuringInsert(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	registeredBlockingDriver.hook = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	t.Cleanup(func() { registeredBlockingDriver.hook = nil })

	raw, err := sql.Open("wzapi-blocking-lidmap", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatalf("dbutil.NewWithDB: %v", err)
	}
	s := NewCachedLIDMap(db)

	batch := make([]store.LIDMapping, 0, 2000)
	for i := 0; i < 2000; i++ {
		batch = append(batch, store.LIDMapping{
			LID: types.JID{User: lidUser(i), Server: types.HiddenUserServer},
			PN:  types.JID{User: pnUser(i), Server: types.DefaultUserServer},
		})
	}

	done := make(chan error, 1)
	go func() { done <- s.PutManyLIDMappings(context.Background(), batch) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("batch insert never reached the DB Exec")
	}

	// The batch is now parked inside DoTxn -> Exec. If the patch is present the
	// cache lock is free here; before the patch it was held for the whole insert.
	locked := make(chan bool, 1)
	go func() {
		ok := s.lidCacheLock.TryLock()
		if ok {
			s.lidCacheLock.Unlock()
		}
		locked <- ok
	}()
	select {
	case ok := <-locked:
		if !ok {
			t.Fatal("PATCH 10 REVERTED: lidCacheLock is held during the batch DB insert — " +
				"an incoming-message PutLIDMapping would stall for the whole history sync")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PATCH 10 REVERTED: could not acquire lidCacheLock while the batch insert was in progress")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("PutManyLIDMappings: %v", err)
	}

	// The cache must be consistent after the insert.
	if got := s.pnToLIDCache[pnUser(0)]; got != lidUser(0) {
		t.Errorf("cache not updated after insert: pnToLIDCache[%s] = %q, want %q", pnUser(0), got, lidUser(0))
	}
	if got := s.lidToPNCache[lidUser(1999)]; got != pnUser(1999) {
		t.Errorf("cache not updated after insert: lidToPNCache[%s] = %q, want %q", lidUser(1999), got, pnUser(1999))
	}
}

func lidUser(i int) string { return "1000000000000" + itoa4(i) }
func pnUser(i int) string  { return "5215500000" + itoa4(i) }

func itoa4(i int) string {
	b := []byte{'0', '0', '0', '0'}
	for p := 3; p >= 0 && i > 0; p-- {
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b)
}
