//! weirdnet-sync 0.8 majority-vote leases as [`Leadership`]: the lease term is the epoch.
use super::Leadership;
use weirdnet_sync::lease::{Elector, LeaseHandle};

/// Held while the elector holds a valid lease; the term is the lease's fencing term.
impl Leadership for Elector {
    fn held(&self) -> Option<u64> {
        self.handle().filter(LeaseHandle::is_valid).map(|h| h.term().0)
    }
}
