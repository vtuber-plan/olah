# How can I contribute to Olah?

Everyone is welcome to contribute, and we value everybody's contribution. Code contributions are not the only way to help the community. Answering questions, helping others, and improving the documentation are also immensely valuable.

It also helps us if you spread the word! Reference the library in blog posts about the awesome projects it made possible, shout out on Twitter every time it has helped you, or simply ⭐️ the repository to say thank you.

However you choose to contribute, please be mindful and respect our code of conduct.

## Ways to contribute

There are lots of ways you can contribute to Olah:
* Submitting issues on Github to report bugs or make feature requests
* Fixing outstanding issues with the existing code
* Implementing new features
* Contributing to the examples or to the documentation

*All are equally valuable to the community.*

#### This guide was heavily inspired by the awesome [transformers guide to contributing](https://github.com/huggingface/transformers/blob/master/CONTRIBUTING.md)

## Tests

Install the project and its test dependencies, then run the deterministic suite:

```sh
python -m pip install -e . -r requirements.txt
python -m pytest tests -m "not live"
```

The two snapshot-download smoke tests contact real Hugging Face repositories and
are explicitly marked `live`. They are skipped unless `OLAH_E2E_LIVE=1` is set;
they are not part of the release gate. To run them and the existing live cache
checks on a machine with Hugging Face access:

```sh
OLAH_E2E_LIVE=1 python -m pytest tests/simple_test.py tests/e2e_cache_live.py
```

## Releasing

1. Update `project.version` in `pyproject.toml` to a new stable `X.Y.Z` version.
2. Commit and push the changes, including the release workflows, to `main`.
3. Tag that exact commit and push the tag (for example, after setting `0.5.2`):

   ```sh
   git tag v0.5.2
   git push origin v0.5.2
   ```

A `v*` tag push starts **Build and publish release artifacts**. The workflow
rejects tags other than `vX.Y.Z` and versions that do not match `pyproject.toml`.
It runs deterministic regression tests, builds one wheel and source distribution,
checks their metadata and smoke-tests the installed wheel, then publishes in order:

1. PyPI (`olah==X.Y.Z`)
2. A GitHub Release with generated notes and the same wheel and source archive
3. Docker Hub and GHCR images for `linux/amd64` and `linux/arm64`, tagged `X.Y.Z`
   and, for the newest stable release, `latest`

There is no need to create a GitHub Release manually. Container publication is a
direct reusable-workflow dependency, so it uses the tagged commit and does not
rely on a release-created event triggering another workflow. Source images from
`dev` remain a separate workflow.

The existing `PYPI_API_TOKEN`, `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` repository
secrets are required. GitHub Release and GHCR publication use the job-scoped
`GITHUB_TOKEN` permissions in the workflows. Push tags from your Git client or an
authorized GitHub App: tag pushes made with a workflow's own `GITHUB_TOKEN` do not
start another push workflow.

Release runs are serialized, queued rather than interrupted (up to GitHub's
100-pending-run limit). An older version retry cannot promote itself over a newer
stable GitHub Release. Keep published tags immutable.

Publication across registries is not transactional. If a later stage fails,
earlier published packages/releases remain available. Prefer **Re-run failed
jobs** on the original run: its tested distribution artifact is retained for 30
days, with separate artifact names for each build attempt. Already-published PyPI filenames must have identical SHA-256 hashes before
they may be skipped. A full rebuild can differ due to build tooling or archive
metadata; if its bytes conflict, recover the original artifacts or release a new
version rather than overwriting that version. The standalone release-image
workflow also supports a manual image-only repair for a published PyPI version
and its existing `vX.Y.Z` source tag; that path never updates `latest`.
