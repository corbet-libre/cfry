# Agent instructions

Use English. Ferry is an LGPL sublibrary of Tor (`ctrn`). It owns bounded
framing, backpressure and stream lifecycle, not network selection, application
policy, records, storage, identities or cryptographic primitives. Reuse maintained
upstream implementations. Keep browser and native message vectors aligned.

No local Cargo, including fmt or metadata. CI uses current stable Rust
and actual Wasm execution; nightly instrumentation gates 100% reachable line
and branch coverage. First-party dependencies follow main, one revision per
crate in lockfiles. No registry publication or payments. Commit explicit paths,
plain English, no AI attribution; pull with rebase before pushing main.
