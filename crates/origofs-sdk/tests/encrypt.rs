//! Encryption at rest through the SDK. `open_local_encrypted` derives the key
//! with Argon2id over a per-store salt file, so the ciphertext round-trips on
//! reopen with the right passphrase, fails loudly on the wrong one, and two
//! stores sharing a passphrase still get independent keys (distinct salts) —
//! which is what makes a weak passphrase expensive to attack and defeats
//! cross-store rainbow tables.

use origofs_sdk::Workspace;

#[tokio::test]
async fn encrypted_workspace_roundtrips_and_persists_a_salt() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("meta.db");
    let cas = dir.path().join("cas");

    {
        let ws = Workspace::open_local_encrypted(&db, &cas, "correct horse battery")
            .await
            .unwrap();
        ws.write("/secret.txt", b"attack at dawn").await.unwrap();
        ws.commit("author", "snapshot").await.unwrap();
    }

    // The salt is created beside the content store (so it survives a DB loss) and
    // is a full 16 bytes.
    let salt = std::fs::read(cas.join("keysalt")).unwrap();
    assert_eq!(salt.len(), 16, "a 16-byte salt is written on first open");

    // Reopen with the same passphrase: the salt is reused, the derived key matches,
    // and the ciphertext decrypts. The salt file is untouched across reopens.
    {
        let ws = Workspace::open_local_encrypted(&db, &cas, "correct horse battery")
            .await
            .unwrap();
        assert_eq!(
            &ws.read("/secret.txt").await.unwrap()[..],
            b"attack at dawn"
        );
        assert_eq!(std::fs::read(cas.join("keysalt")).unwrap(), salt);
    }
}

#[tokio::test]
async fn wrong_passphrase_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("meta.db");
    let cas = dir.path().join("cas");

    {
        let ws = Workspace::open_local_encrypted(&db, &cas, "right passphrase")
            .await
            .unwrap();
        ws.write("/secret.txt", b"attack at dawn").await.unwrap();
        ws.commit("author", "snapshot").await.unwrap();
    }

    // Same store (same salt), different passphrase → a different key → the AEAD tag
    // won't verify. Reads fail rather than returning garbage.
    let ws = Workspace::open_local_encrypted(&db, &cas, "wrong passphrase")
        .await
        .unwrap();
    let msg = ws.read("/secret.txt").await.unwrap_err().to_string();
    assert!(
        msg.contains("decryption failed"),
        "wrong passphrase should fail loudly, got: {msg}"
    );
}

#[tokio::test]
async fn two_stores_sharing_a_passphrase_get_independent_salts() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let pass = "the same passphrase everywhere";

    let _wa = Workspace::open_local_encrypted(a.path().join("m.db"), a.path().join("cas"), pass)
        .await
        .unwrap();
    let _wb = Workspace::open_local_encrypted(b.path().join("m.db"), b.path().join("cas"), pass)
        .await
        .unwrap();

    let sa = std::fs::read(a.path().join("cas").join("keysalt")).unwrap();
    let sb = std::fs::read(b.path().join("cas").join("keysalt")).unwrap();
    assert_ne!(
        sa, sb,
        "each store gets its own random salt, so a shared passphrase yields different keys"
    );
}
