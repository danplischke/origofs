//! `Workspace`'s Python bindings, one module per topic.
//!
//! This was a single `#[pymethods] impl Workspace` block of 4,166 lines and 211
//! methods — the largest single item in the workspace, and the place a reviewer
//! stops reading. The file already carried section markers for most of it; they
//! are modules now, and the 108 methods that sat above the first marker are
//! grouped for the first time.
//!
//! Splitting one `#[pyclass]` across several `#[pymethods]` blocks needs pyo3's
//! `multiple-pymethods` feature, which is why `Cargo.toml` turns it on. It costs
//! a build dependency on `inventory` and is unavailable on wasm, which origofs
//! does not target; the three platforms CI builds all support it.
//!
//! Every method moved verbatim. Nothing about the Python surface changes — same
//! class, same names, same signatures.

mod acl;
mod actors;
mod admin;
mod coedit;
mod collab;
mod files;
mod history;
mod mounts;
mod open;
