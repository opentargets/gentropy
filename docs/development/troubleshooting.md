---
title: Troubleshooting
---

# Troubleshooting

## BLAS/LAPACK

If you see errors related to BLAS/LAPACK libraries, see [this StackOverflow post](https://stackoverflow.com/questions/69954587/no-blas-lapack-libraries-found-when-installing-scipy) for guidance.

## UV

The default python version and gentropy dependencies are managed by (uv)[https://docs.astral.sh/uv/]. To perform a fresh installation run `make setup-dev`.

## Java

Officially, PySpark requires Java version 8 (a.k.a. 1.8) or above to work. However, if you have a very recent version of Java, you may experience issues, as it may introduce breaking changes that PySpark hasn't had time to integrate. For example, as of May 2023, PySpark did not work with Java 20.

If you are encountering problems with initializing a Spark session, try using Java 11.

## Pre-commit

If you see an error message thrown by pre-commit, which looks like this (`SyntaxError: Unexpected token '?'`), followed by a JavaScript traceback, the issue is likely with your system NodeJS version.

One solution which can help in this case is to upgrade your system NodeJS version. However, this may not always be possible. For example, Ubuntu repository is several major versions behind the latest version as of July 2023.

Another solution which helps is to remove Node, NodeJS, and npm from your system entirely. In this case, pre-commit will not try to rely on a system version of NodeJS and will install its own, suitable one.

On Ubuntu, this can be done using `sudo apt remove node nodejs npm`, followed by `sudo apt autoremove`. But in some cases, depending on your existing installation, you may need to also manually remove some files. See [this StackOverflow answer](https://stackoverflow.com/a/41057802) for guidance.

After running these commands, you are advised to open a fresh shell, and then also reinstall Pyenv and Poetry to make sure they pick up the changes (see relevant section above).

## MacOS

Some functions on MacOS may throw a java error:

`python3.10/site-packages/py4j/protocol.py:326: Py4JJavaError`

This can be resolved by adding the follow line to your `~/.zshrc`:

`export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`

## Creating development dataproc cluster (OT users only)

To start dataproc cluster in the development mode run

```
make create-dev-cluster
```

The command above will prepare 3 different resources:

- gentropy package
- cli script
- cluster setup script

and based on the branch ref (for example `dev`) will create a namespaced folder under GCS (`gs://genetics_etl_python_playground/initialisation/gentropy/dev`) with the three files described above. These files will be then used to create the cluster environment.
