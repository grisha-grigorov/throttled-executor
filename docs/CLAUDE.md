# docs

Loaded when a file under `docs/` is touched.

- The root `README.md` is the user-facing design document and tutorial. `TutorialTests`
  in core are its migration recipes as runnable code — change both together.
- `docs/api-reference.md` lists every exported type and method. Package-private members
  and the `internal` package are omitted. When it and the sources disagree, the source
  wins and this file is fixed.
- `docs/architecture.md` is six Mermaid diagrams drawn from the sources (executor tree,
  policies, listeners, lifecycle, JPMS modules, state machine). Same rule: source wins,
  fix the diagram. It currently lives on the `docs/architecture-diagrams` branch.
- Each satellite's `README.md` is that satellite's contract; the root README links to them.
- Javadoc in exported types refers to internal ones with `{@code}`, never `{@link}`.
