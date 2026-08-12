---
name: Bug report
about: Report a defect in MatrixRaft
labels: bug
---

**Description**

A clear description of the bug.

**Expected vs. actual behavior**

**Reproduction**

A minimal code snippet or failing test, if possible.

**Environment**

- matrixraft version / commit:
- Rust version (`rustc --version`):
- OS:

**Safety relevance**

Does this involve read-index, lease, applied-index-fence, or membership safety? If
so, describe the incorrect behavior precisely (e.g., stale read, split-brain).
