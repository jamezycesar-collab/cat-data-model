# Branch protection runbook

Branch protection turns the CI guardrails (`validate_event_taxonomy.py`, `validate_no_fabrications.py`, `validate_python_syntax.py`) from advisory checks into hard merge gates. Without it, a contributor can merge a PR that fails CI by clicking "Merge anyway."

This is a one-time configuration in the GitHub web UI; the API token used by Cowork doesn't have the `administration:write` permission needed to set it programmatically.

## Steps (about 5 minutes)

### 1. Open the repository settings

Navigate to https://github.com/jamezycesar-collab/cat-data-model/settings/branches and sign in.

### 2. Add a branch protection rule

Click **Add branch protection rule** (or **Add rule** depending on UI version).

In **Branch name pattern**, type:

```
main
```

### 3. Enable the gates

Tick each of the following:

- [x] **Require a pull request before merging**
  - [x] Require approvals (set to 1)
  - [x] Require review from Code Owners
  - [x] Dismiss stale pull request approvals when new commits are pushed
- [x] **Require status checks to pass before merging**
  - [x] Require branches to be up to date before merging
  - In the search box, type `validate` and tick the workflow that appears (named "Validate taxonomy and check for fabrications" from `.github/workflows/validate-taxonomy.yml`)
- [x] **Require conversation resolution before merging**
- [x] **Do not allow bypassing the above settings**

### 4. Save

Click **Create** at the bottom of the form.

## Verify

Open a test branch and push a commit that intentionally adds a fabricated code (e.g. add `MEFAKE` to `primary-sources/cat_im_event_types.csv`). Open a PR. The PR should:

- Show a red X next to the workflow run
- Display "Required" next to "Validate taxonomy and check for fabrications"
- Disable the green "Merge pull request" button until either the failure is fixed or the rule is bypassed by an administrator

After confirming the gate works, delete the test branch.

## What this protects

| File / path | Protection | Why |
|-------------|-----------|-----|
| `primary-sources/*` | CODEOWNER approval + CI | These files are the source of truth for the regulatory taxonomy |
| `guardrails/spec_pins.json` | CODEOWNER approval + CI | The SHA-256 pins gate every other check |
| `guardrails/validate_*.py` | CODEOWNER approval + CI | The validators themselves; they shouldn't be silenced |
| `.github/workflows/` | CODEOWNER approval + CI | The CI itself; disabling a workflow is a self-pwn |
| All Python files | CI (`validate_python_syntax.py`) | No pseudo-code can land |
| Whole tree | CI (`validate_no_fabrications.py`) | No fabricated codes outside audit context |

## Reverting

If you ever need to push a hotfix that bypasses CI, the GitHub UI lets you:

1. Temporarily uncheck "Do not allow bypassing the above settings" (admin only)
2. Push and merge the hotfix
3. Re-check the box

Document any bypass in `docs/overrides/<date>.md` per `guardrails/VERIFICATION_PROTOCOL.md`.
