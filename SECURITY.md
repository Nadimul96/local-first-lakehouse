# Security

This project encrypts data on the client and manages its own keys, so its security model
is worth stating plainly.

## What it protects

- **Confidentiality at rest in object storage.** Every object is sealed with AES-256-GCM
  before upload. The storage provider, the catalog and anyone with bucket access see
  ciphertext, SHA-256 hashes, sizes and object names (which carry the event date and segment
  number). They do not see event contents.
- **Integrity end to end.** The GCM tag covers the ciphertext and a canonical header (key
  id, plaintext hash, object key). Every object is read back and verified before it is
  considered durable and again before any local copy is deleted.
- **Secrets in captured data.** Credentials and PII matching the redaction rules are
  replaced before anything is written to disk, with HMAC-SHA256 tags under a per-install
  key so a short secret cannot be recovered from its tag by brute force.
- **Append-only history.** Receipts, catalog rows, tiering records and sealed segments
  reject UPDATE and DELETE at the database level. The optional Postgres role has only
  SELECT and INSERT.

## What it does not protect

- **The local machine.** Keys live in `LAKEHOUSE_HOME/keys` (mode 0600, directory 0700),
  in environment variables, or in the macOS Keychain. Anyone who can read them, or the
  local event log before reclamation, can read the data.
- **Metadata.** Object names, sizes, timing and key ids are visible remotely.
- **Redaction is pattern-based.** It catches common credential formats, emails, card
  numbers (Luhn-checked), SSNs and high-entropy tokens. It is a safety net, not a DLP system.
- **The Keychain provider** passes the generated key to `/usr/bin/security` as an argument
  once at creation, which is briefly visible to other local processes.

## Keys

- `lakehouse rotate-key` creates a new data key for new objects. Old keys are kept so
  existing objects stay readable; deleting an old key makes its objects undecryptable.
- 96-bit nonces are random per object. Rotate long before one key encrypts 2^32 objects.

## Reporting

Please report suspected vulnerabilities privately through GitHub's
"Report a vulnerability" (Security advisories) on this repository rather than in a public
issue.
