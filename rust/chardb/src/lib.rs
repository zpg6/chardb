#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod error;
mod operation;
pub mod wire;

#[cfg(feature = "introspection")]
pub mod introspection;

#[cfg(feature = "client")]
mod client;

pub use error::{Error, ErrorKind, Result};
pub use operation::{Mutation, Operation, Query};

#[cfg(feature = "client")]
pub use client::{ClientConfig, ConnectionState, SubscriptionEvent};

#[cfg(feature = "sync")]
pub use client::{Client, Subscription};

#[cfg(feature = "async")]
pub use client::{AsyncClient, AsyncSubscription};

#[cfg(feature = "browser-login")]
pub mod browser_login;
