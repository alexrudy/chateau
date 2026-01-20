//! Simple locking primitaves for working with `Arc<Mutex<T>>`
//!

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use std::sync::Weak;

use parking_lot::Mutex;

use crate::DebugLiteral;

pub(crate) struct WeakOpt<T>(Option<Weak<T>>);

impl<T> WeakOpt<T> {
    pub(crate) fn none() -> Self {
        Self(None)
    }

    pub(crate) fn downgrade(arc: &Arc<T>) -> Self {
        Self(Some(Arc::downgrade(arc)))
    }

    pub(crate) fn upgrade(&self) -> Option<Arc<T>> {
        self.0.as_ref().and_then(|weak| weak.upgrade())
    }

    #[allow(dead_code)]
    pub(crate) fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

impl<T> Clone for WeakOpt<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> fmt::Debug for WeakOpt<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(_) => f
                .debug_tuple("WeakOpt")
                .field(&DebugLiteral("Some(...)"))
                .finish(),
            None => f
                .debug_tuple("WeakOpt")
                .field(&DebugLiteral("None"))
                .finish(),
        }
    }
}

/// Weak reference to a mutex.
pub struct WeakMutex<T> {
    inner: WeakOpt<Mutex<T>>,
}

impl<T> fmt::Debug for WeakMutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WeakMutex").field(&self.inner).finish()
    }
}

impl<T> WeakMutex<T> {
    #[allow(dead_code)]
    pub(in crate::client) fn downgrade(target: &ArcMutex<T>) -> WeakMutex<T> {
        Self {
            inner: WeakOpt::downgrade(&target.0),
        }
    }

    pub(in crate::client) fn none() -> Self {
        Self {
            inner: WeakOpt::none(),
        }
    }

    #[allow(dead_code)]
    pub(in crate::client) fn try_lock(&self) -> Option<ArcMutexGuard<T>> {
        self.inner
            .upgrade()
            .and_then(|inner| inner.try_lock_arc().map(ArcMutexGuard))
    }

    pub(in crate::client) fn lock(&self) -> Option<ArcMutexGuard<T>> {
        self.inner
            .upgrade()
            .map(|inner| ArcMutexGuard(inner.lock_arc()))
    }

    #[allow(dead_code)]
    pub(in crate::client) fn is_none(&self) -> bool {
        self.inner.is_none()
    }
}

impl<T> Clone for WeakMutex<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Reference-counted, mutex-protected data.
#[derive(Debug)]
pub struct ArcMutex<T>(Arc<parking_lot::Mutex<T>>);

impl<T> Clone for ArcMutex<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> ArcMutex<T> {
    /// Create a new ArcMutex with the given value.
    pub fn new(value: T) -> Self {
        Self(Arc::new(parking_lot::Mutex::new(value)))
    }

    /// Lock the ArcMutex and return a guard.
    pub fn lock(&self) -> ArcMutexGuard<T> {
        ArcMutexGuard(self.0.lock_arc())
    }
}

/// ArcMutexGuard is a guard that provides mutable access to the value inside the ArcMutex.
#[allow(missing_debug_implementations)]
pub struct ArcMutexGuard<T>(parking_lot::ArcMutexGuard<parking_lot::RawMutex, T>);

impl<T> ArcMutexGuard<T> {
    /// Downgrade the underlying mutex to a weak reference.
    pub fn downgrade(&self) -> WeakMutex<T> {
        WeakMutex {
            inner: WeakOpt::downgrade(&parking_lot::ArcMutexGuard::mutex(&self.0)),
        }
    }
}

impl<T> Deref for ArcMutexGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for ArcMutexGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
pub(crate) mod test_weak_opt {
    use super::*;

    #[test]
    fn weak_opt() {
        let arc = Arc::new(());
        let weak = WeakOpt::downgrade(&arc);
        assert!(weak.upgrade().is_some());
        drop(arc);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn weak_opt_none() {
        let weak = WeakOpt::<()>::none();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn weak_opt_debug() {
        let arc = Arc::new(());
        let weak = WeakOpt::downgrade(&arc);
        assert_eq!(format!("{weak:?}"), "WeakOpt(Some(...))");

        let weak: WeakOpt<()> = WeakOpt::none();
        assert_eq!(format!("{weak:?}"), "WeakOpt(None)");
    }
}
