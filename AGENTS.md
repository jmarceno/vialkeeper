# Contributor instructions

This file contains repository-safe guidance only.

Do not add internal specifications, private document identifiers, personal
filesystem paths, credentials, tokens, or other non-public project material to
tracked files.

## Validation

- Keep test guarantees intact; never weaken checks, thresholds, or coverage to make a gate pass.
- Use `mix check.fast` while iterating.
- Run `mix check.full` before handoff. A change is incomplete while that gate is red.
- Use the narrowest focused checks that cover the files being changed during iteration.

## Documentation

- Keep `README.md` focused on the public product and developer interface.
- Keep `Operations.md` focused on public deployment and operator procedures.
- Update the appropriate public document when a new public feature changes its contract.
- Do not document bug fixes in those files unless the public contract itself changed.

## Elixir

- Normalize external maps once at the boundary.
- Prefer pattern matching and function clauses over defensive branching.
- Handle `{:ok, _} | {:error, _}` results explicitly.
- Do not blanket-rescue exceptions or use unsupervised production concurrency.
- Pass explicit timeouts to blocking calls.
- Keep backend-specific APIs inside their boundary modules.
- Add precise `@spec`s to new public APIs.

Do not publish or push changes automatically. Review the complete diff and
remove any secrets or personal data before distribution.
