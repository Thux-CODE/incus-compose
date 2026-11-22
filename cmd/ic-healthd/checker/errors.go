package checker

import "github.com/lxc/incus-compose/ievent/iutil"

// ErrInstanceNoHealthcheck indicates an instance has no healthcheck.
var ErrInstanceNoHealthcheck = iutil.NewError("instance has no healthcheck")

// ErrInstanceNotEnabled means the instance looks like it wants health checking but never opted in.
var ErrInstanceNotEnabled = iutil.NewError("instance has a healthcheck but is not enabled")

// ErrIntentionallyStopped means the user stopped the instance, so no restart
// policy may bring it back.
var ErrIntentionallyStopped = iutil.NewError("the instance has been intentionally stopped")

// ErrNotEnriched is an instance event that came without the read of the
// instance. The enricher passes one on as it is for an earlier stream session
// only: a leftover, which says nothing of the instance now.
var ErrNotEnriched = iutil.NewError("event without an instance read")

// ErrNotRunning is an internal sentinel error.
var ErrNotRunning = iutil.NewError("not running")
