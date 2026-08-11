Always use elixir-safe-code skill when writing code.

When receiving a document as a wiki link, doc-id, folder-id, column-id, card-id or event-id, this means that you should retrieve the data from the user's UnboundMark workspace, use the UnboundMark MCP to retrieve the document.

**Authoritative source docs and plans are inside folder `folder-id:4007e0d9-3cf2-4f17-a694-5680200d6547` in UnboundMark.**
**All supporting docs must be created and maintained inside UnboundMark and not on this repo. Execeptions are READMEs and Operations.md, those docs must be kept local**

Never weaken test guarantees.

Use the validation ladder in elixir-safe-coding: focused checks while iterating; full project gate only before handoff or wave commit. Adversarial review uses severity tiers (BLOCKER / PLAN_GAP / NIT); do not restart full review for NITs alone.

When editing any documents locally or user UnboundMark workspace, you must not make edits or comments — you should integrate the changes as if they were always there, user may tell you to break this rule, this is fine and if explicitly asked, you should break this rule. This will show any conflict or incompatibility.

**This in an unreleased product no backward compatibility is needed.**

We do not work with PRs or any other GitHub features.

**NEVER PUSH ANYTHING IF NOT EXPLICITLY ASKED.**

**Never add  `@moduledoc false` just to satisfy requirements, every ` @moduledoc false` must be justifiable, otherwise insert a real documentation.**

Never add comments with the plan/wave/phase that a code satisfies, this kind of comment is strictly forbidden.
