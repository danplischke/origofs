//! Low-level accessors that interop layers build on (`docs/DESIGN.md` §4c).
//!
//! The git bridge in `origofs-git` needs to reach *below* the path-oriented engine:
//! read and write raw objects in the content store, reassemble or store whole
//! file bodies, decode object-graph nodes by hash, and read/point branch refs.
//! These are the stable seams it uses; everything git-specific (object encoding,
//! packfiles, LFS) lives in `origofs-git` so origofs-core stays free of git deps.

use crate::chunk::Manifest;
use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::objectgraph::{Commit, Tree};
use crate::types::Hash;
use bytes::{Bytes, BytesMut};

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Fetch a raw object (tree/commit/symlink-target/chunk) by its content address.
    pub async fn get_object(&self, hash: &Hash) -> Result<Bytes> {
        self.content.get(hash).await
    }

    /// Store a raw object, returning its content address.
    pub async fn put_object(&self, bytes: &[u8]) -> Result<Hash> {
        self.content.put(bytes).await
    }

    /// Reassemble a whole file body from its blob-manifest hash.
    pub async fn read_blob_bytes(&self, manifest_hash: &Hash) -> Result<Bytes> {
        let manifest = self.load_manifest(manifest_hash).await?;
        let mut buf = BytesMut::with_capacity(manifest.size as usize);
        for c in &manifest.chunks {
            buf.extend_from_slice(&self.content.get(&c.hash).await?);
        }
        Ok(buf.freeze())
    }

    /// Store a whole file body, returning its blob-manifest hash. Empty bodies
    /// hash the default (empty) manifest so trees referencing them stay valid.
    pub async fn store_blob_bytes(&self, data: &[u8]) -> Result<Hash> {
        match self.store_body(data).await? {
            (Some(h), _) => Ok(h),
            (None, _) => self.content.put(&Manifest::default().encode()).await,
        }
    }

    /// Decode the commit object at `hash`.
    pub async fn commit_object(&self, hash: &Hash) -> Result<Commit> {
        Commit::decode(&self.content.get(hash).await?)
    }

    /// Decode the tree object at `hash`.
    pub async fn tree_object(&self, hash: &Hash) -> Result<Tree> {
        Tree::decode(&self.content.get(hash).await?)
    }

    /// The commit a branch ref points at, if any.
    pub async fn branch_head(&self, branch: &str) -> Result<Option<Hash>> {
        Ok(self
            .meta
            .get_ref(branch)
            .await?
            .and_then(|v| Hash::from_hex(&v)))
    }

    /// Point a branch ref at `hash` (creating the ref if absent).
    pub async fn set_branch(&self, branch: &str, hash: Hash) -> Result<()> {
        self.meta.set_ref(branch, &hash.to_hex()).await
    }
}
