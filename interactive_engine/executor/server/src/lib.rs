extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

extern crate core;
extern crate rand;

pub mod proxy;
pub mod queries;
pub mod request;

#[cfg(not(feature = "gcip"))]
mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }

    pub mod protocol {
        tonic::include_proto!("protocol");
    }

    pub mod procedure {
        tonic::include_proto!("procedure");
    }
}

#[rustfmt::skip]
#[cfg(feature = "gcip")]
mod generated {
    #[path = "common.rs"]
    pub mod common;

    #[path = "protocol.rs"]
    pub mod protocol;

    #[path = "procedure.rs"]
    pub mod procedure;
}

pub use generated::procedure;
pub use generated::protocol as pb;
