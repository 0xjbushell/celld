# Azure Blob Storage

Phase: v1

Canonical spec: [`spec.md`](spec.md)

Decision log: [`decisions.tsv`](decisions.tsv)

Upstream target:
[`denoland/celld#139`](https://github.com/denoland/celld/issues/139), delivered
through the repository's documented focused `git format-patch` email flow.

Tracker parent/ref:
[`0xjbushell/celld#1`](https://github.com/0xjbushell/celld/issues/1)

Ready frontier: complete. Issues
[`#2`](https://github.com/0xjbushell/celld/issues/2) through
[`#6`](https://github.com/0xjbushell/celld/issues/6) are accepted.

Local blockers: none. Tenant policy prevents live creation of a
service-principal password, so client-secret selection and no-fallback behavior
are qualified by committed tests rather than a live password credential.

Accepted: [`#2`](https://github.com/0xjbushell/celld/issues/2) was delivered
as commit `896c5393af5e5b97ff930e1b3978627b3175f20c`; core Azure issue
[`#3`](https://github.com/0xjbushell/celld/issues/3) was delivered as commit
`f8afab8fbf39dfee6c9f25e6e0230bea10231796`; official identity issue
[`#4`](https://github.com/0xjbushell/celld/issues/4) was delivered as commit
`1da23e7a7180b389b15d421d07adee590d7f56c0`; failure qualification issue
[`#6`](https://github.com/0xjbushell/celld/issues/6) was delivered as commit
`679fe33585648d9cf3fcade35a80725f574d9b20`. Multi-node issue
[`#5`](https://github.com/0xjbushell/celld/issues/5) passed in AKS. The focused
two-patch core series and clean four-patch upstream series are preserved in the
session delivery artifacts.

Next action: submit the reviewed patch series through the upstream email
contribution flow and decide whether to migrate or extend the expiring Azure
proof resource group before keeping the AKS lab long term.
