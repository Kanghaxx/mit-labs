package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockName string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockName: l,
		clientID: kvtest.RandValue(8)}
	ck.Put(l, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, _ := lk.ck.Get(lk.lockName)
		if value == lk.clientID {
			return // the same client could already get the lock but receive ErrMaybe because of response loss
		}
		if value == "" {
			err := lk.ck.Put(lk.lockName, lk.clientID, version)
			if err == rpc.OK {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.lockName)
	if value == lk.clientID { // release must be called only by the lock holder
		lk.ck.Put(lk.lockName, "", version)
	}
}
