//! General tower services used for composition in this module.

mod addressable;
mod make;
mod serviceref;
mod shared;

pub use self::addressable::{
    ResolvedAddressableFuture, ResolvedAddressableLayer, ResolvedAddressableService,
};
pub use self::make::{BoxMakeServiceLayer, BoxMakeServiceRef, MakeServiceRef, make_service_fn};
pub use self::serviceref::ServiceRef;
pub use self::shared::SharedService;
