---
title: Troubleshooting
---

# Troubleshooting

## BLAS/LAPACK

If you see errors related to BLAS/LAPACK libraries, see [this StackOverflow post](https://stackoverflow.com/questions/69954587/no-blas-lapack-libraries-found-when-installing-scipy) for guidance.

## UV

The default python version and gentropy dependencies are managed by [uv](https://docs.astral.sh/uv/). To perform a fresh installation run `make setup-dev`.

## Adding new dependencies or updating existing ones

To add new dependencies or update existing ones, you need to update the `pyproject.toml` file. This can be done automatically with `uv add ${package}` command. Refer to the [uv documentation](https://docs.astral.sh/uv/) for more information.

## Java

Officially, PySpark requires Java version 8, or 11, 17. To support hail (gentropy dependency) it is recommended to use Java 11.

## Pre-commit

If you see an error message thrown by pre-commit, which looks like this (`SyntaxError: Unexpected token '?'`), followed by a JavaScript traceback, the issue is likely with your system NodeJS version.

One solution which can help in this case is to upgrade your system NodeJS version. However, this may not always be possible. For example, Ubuntu repository is several major versions behind the latest version as of July 2023.

Another solution which helps is to remove Node, NodeJS, and npm from your system entirely. In this case, pre-commit will not try to rely on a system version of NodeJS and will install its own, suitable one.

On Ubuntu, this can be done using `sudo apt remove node nodejs npm`, followed by `sudo apt autoremove`. But in some cases, depending on your existing installation, you may need to also manually remove some files. See [this StackOverflow answer](https://stackoverflow.com/a/41057802) for guidance.

## MacOS

Some functions on MacOS may throw a java error:

`python3.10/site-packages/py4j/protocol.py:326: Py4JJavaError`

This can be resolved by adding the follow line to your `~/.zshrc`:

`export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`

## Creating development dataproc cluster (OT users only)

To start dataproc cluster in the development mode run

```bash
make create-dev-cluster
```

!!! note "Tip"
This command will work, provided you have fully commited and pushed all your changes to the remote repository.

The command will create a new dataproc cluster with the following configuration:

- package installed from the current branch you are checkout on (for example `dev` or `feature/xxx`)
- uv installed in the cluster (to speed up the installation and dependency resolution process)
- cli script to run gentropy steps

This process requires gentropy to be installable by git repository - see VCS support in [pip documentation](https://pip.pypa.io/en/stable/topics/vcs-support/).
