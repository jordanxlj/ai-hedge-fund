# CI/CD configuration

This directory contains configuration files used by the GitHub Actions workflows.

## Multi-repo build

Trigger the `cicd.yml` workflow manually with `run_multirepo=true` to build
multiple repositories. Configure the list in `multirepo.json`:

```
{
  "repositories": [
    {
      "repo": "org/other-repo",
      "ref": "main",
      "path": "deps/other-repo",
      "build": [
        "make build VERSION=${CICD_VERSION}"
      ]
    }
  ]
}
```

Fields:
- `repo`: `owner/name` (required)
- `ref`: git ref (branch/tag/sha), optional
- `path`: workspace path relative to the multi-repo workspace root
- `build`: list of shell commands executed in the repo directory

For private repositories, set the `CICD_GH_TOKEN` secret so CI can clone them.

## Version tagging

The `cicd.yml` workflow can create a git tag based on the version in
`pyproject.toml` or the `release_version` input.

## Hardware validation

The `hardware-validation.yml` workflow uses self-hosted runners with board
access. Set the command inputs or environment variables used by the scripts in
`scripts/cicd`.
