# Releasing

Current candidate: **0.1.0a2**, an experimental alpha. Preparing this workflow or
running its default `check` mode does not upload anything to PyPI or TestPyPI.
Do not claim a release exists until its index page and a fresh install have been
verified. The repository's GitHub releases and Python package indices are
separate services.

## One-time account setup

1. Register at [PyPI](https://pypi.org/account/register/), verify your email and
   enable [two-factor authentication](https://docs.pypi.org/your-account/two-factor-authentication/).
   Keep recovery codes and authenticator secrets private.
2. For an upload rehearsal, register separately at
   [TestPyPI](https://test.pypi.org/account/register/). Accounts and publisher
   registrations do not carry over between the two indices.
3. In the repository's **Settings → Environments**, create `pypi` and `testpypi`.
   Restrict deployments to release tags (`v*`) and configure a trusted maintainer
   as a required reviewer. Check that the approval settings allow your actual
   maintainer arrangement to approve a release; a sole maintainer cannot supply
   a second independent approver. These settings are configured in GitHub, not
   created or protected just by mentioning an environment in YAML.
4. Register a **pending publisher** at each applicable index. Fill these fields
   exactly; the PyPI account username need not match the GitHub owner:

| Field | PyPI | TestPyPI |
| --- | --- | --- |
| Configuration page | [PyPI publishing](https://pypi.org/manage/account/publishing/) | [TestPyPI publishing](https://test.pypi.org/manage/account/publishing/) |
| PyPI project name | `pytest-stream-lifecycle` | `pytest-stream-lifecycle` |
| Owner | `shenxi200-eng` | `shenxi200-eng` |
| Repository name | `pytest-stream-lifecycle` | `pytest-stream-lifecycle` |
| Workflow name | `release.yml` | `release.yml` |
| Environment name | `pypi` | `testpypi` |

The workflow field is the **filename**, not its display name or full path.
A pending publisher does not reserve the package name. A missing project page
also does not guarantee the name is available; final validation happens at
registration/upload. No password or long-lived API token is needed in GitHub.
See [PyPI's pending publisher guide](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [Trusted Publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

## Check without publishing

Once the preparation commit is on `main`:

```sh
gh workflow run release.yml --ref main -f target=check
```

Alternatively, open **Actions → Release preparation → Run workflow** and select `check`.
This validates, runs the existing cross-platform CI matrix, builds wheel and
sdist, checks package metadata, and runs tests against a fresh wheel installation.
Inspect the run and download its distribution artifact. Both publishing jobs
must be **skipped**. This checks the pipeline, not PyPI account ownership or OIDC
upload authentication.

## Publish a candidate

Only do this after the maintainer approves the upload and the index bindings and
GitHub environments are configured:

1. Review the final source commit. Set the intended version in `pyproject.toml`,
   update the changelog, and reconcile the README's pre-publication status and
   install instructions. Keep the alpha version unless stability has actually
   been established. Re-run `check` on that exact commit.
2. Create and push an annotated tag matching the version, for example
   `v0.1.0a2`, on the reviewed commit in `main`'s history. Do not move a published
   version's tag. The workflow checks tag/version agreement and main ancestry.
3. Run the workflow on the tag with `target=testpypi`, review the tests and build,
   then approve the TestPyPI environment deployment. Example:

   ```sh
   gh workflow run release.yml --ref v0.1.0a2 -f target=testpypi
   ```

4. Verify the TestPyPI project, version, artifact hashes and provenance. In a
   fresh virtual environment, install runtime dependencies from normal PyPI,
   then obtain just this candidate from TestPyPI:

   ```sh
   python -m pip install 'pytest>=8,<10' 'uvicorn>=0.30,<1' 'httpx>=0.27,<1' 'httpx-sse>=0.4,<0.5'
   python -m pip install --no-deps --index-url https://test.pypi.org/simple/ pytest-stream-lifecycle==0.1.0a2
   python -m pip check
   ```

   Run a fixture-discovery smoke test and a real streaming example. Avoid mixing
   both indices through `--extra-index-url`; TestPyPI need not mirror dependencies.
5. With explicit approval to publish the final candidate, run on the **same tag**
   with `target=pypi` and approve the `pypi` environment. Each dispatch builds and
   tests its own distribution; the production job publishes exactly that run's
   tested artifacts. It does not promote previously uploaded TestPyPI bytes.
6. Verify [the PyPI project](https://pypi.org/project/pytest-stream-lifecycle/),
   download hashes/provenance and a clean installation:

   ```sh
   python -m pip install pytest-stream-lifecycle==0.1.0a2
   python -m pip check
   ```

   An exact alpha-version pin selects the prerelease explicitly. Announce it as
   experimental, not a stable release.

## Failure handling and trust boundary

Build/test jobs have no OIDC publishing permission. Each separate publishing job
only downloads the same run's distribution artifact and invokes the pinned PyPA
action; it does not check out or build the source. OIDC permission is scoped to
those jobs, with a fixed index URL and environment. Attestations are enabled.
Protect repository write access, workflow changes and release tags: an editor who
can change the release workflow can change its in-code safeguards. Environment
review and PyPI's exact publisher binding are independent controls.

Uploads are not transactional. If an upload partly succeeds, inspect index files
and hashes before deciding how to recover. The workflow does not silently skip
existing files. PyPI files cannot be replaced by uploading different content with
the same filename; correct a bad release with a new version, and consider yanking
it rather than moving a tag or pretending the old upload never happened.
