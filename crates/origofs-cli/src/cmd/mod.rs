//! One module per topic, one function per subcommand.
//!
//! `main` was a 1,186-line function with 54 match arms — the whole CLI in one
//! body, where finding a command meant scrolling and adding one meant appending
//! to the longest function in the workspace. `main` now parses, opens the
//! workspace, and dispatches; each arm's code lives beside the other commands a
//! reader would compare it to.
//!
//! The bodies moved verbatim. What changed is where they are and that each one
//! now has a signature naming exactly what it needs — most take the workspace
//! and their own arguments and nothing else, which was not visible while they
//! all shared `main`'s scope.

pub mod admin;
pub mod attribution;
pub mod files;
pub mod history;
pub mod surfaces;
