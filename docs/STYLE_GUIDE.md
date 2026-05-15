<a id="top"></a>

# Documentation Style Guide

> Formatting principles for all markdown files in this project.

---

## Header Hierarchy

 Level | Usage | Example |
:-----:|-------|---------|
 `#` | Document title (one per file) | `# Architecture` |
 `##` | Major sections | `## Module Map` |
 `###` | Subsections | `### Framework` |
 `####` | Detail items (e.g., Bash/PowerShell) | `#### Bash` |

**Rules:**
- Every file starts with `<a id="top"></a>` followed by the `#` title
- A `>` blockquote summary follows the title
- `---` horizontal rule separates the title block from the TOC

---

## Table of Contents

Every file with **3+ sections** gets a TOC immediately after the title block.

```markdown
## Table of Contents

 Section | Description |
---------|-------------|
 [Section Name](#anchor) | Brief description |
```

**Anchor links** use GitHub-compatible auto-generated IDs:
- Lowercase, spaces → hyphens, special chars removed
- For custom anchors: `<a id="custom-id"></a>` on the line before the heading

---

## Navigation Links

**Back-to-top** link at the end of each `##` section:

```html
<p align="right"><a href="#top">↑ back to top</a></p>
```

**Back-to-group** link (for documents with grouped subsections like SCENARIOS.md):

```html
<p align="right"><a href="#top">↑ top</a> · <a href="#group-1">↑ group</a></p>
```

---

## Tables

- Always include a header row and separator
- Align numeric columns to the right with `:` in separator
- Use `|:---:|` for centered columns (flags, status, priority)

```markdown
 Column | Type | Description |
--------|------|-------------|
 `name` | STRING | The thing |
```

---

## Emphasis Conventions

 Element | Format | Example |
---------|--------|---------|
 Column names | Backtick | `feed_key` |
 Table names | Backtick | `ops_file_inventory` |
 File paths | Backtick | `src/framework/constants.py` |
 Config values | Backtick | `Y`, `FILE_MODIFIED_TS` |
 Functions | Backtick with parens | `run_dispatcher()` |
 Key terms | **Bold** | **FULL tier** |
 Status labels | Emoji prefixed | ✅ Done, 🔴 High, 🟡 Medium, 🟢 Low |
 Warnings / traps | `>` blockquote | > **Trap:** description |
 Key lessons | `>` blockquote | > **Key lesson:** description |

---

## Priority & Impact Labels

 Label | Meaning |
:-----:|---------|
 🔴 High | Blocks onboarding or causes data issues |
 🟡 Medium | Quality-of-life or operational efficiency |
 🟢 Low | Nice-to-have, rare edge case |

---

## Section Separators

- `---` between major sections (after nav links, before next `##`)
- No `---` between subsections (`###`) within the same group

---

## Code Blocks

- Always specify language: ` ```python `, ` ```sql `, ` ```bash `, ` ```powershell `
- Use `####` subheader (not `#` comment) when showing Bash vs PowerShell alternatives *outside* code blocks
- `# comment` inside code blocks is fine (it's a shell/Python comment)

<p align="right"><a href="#top">↑ back to top</a></p>
