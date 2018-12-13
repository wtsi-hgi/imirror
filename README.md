# imirror

Mirror a directory on a POSIX filesystem into an iRODS collection,
distributing the upload over an LSF cluster.

## Usage

    imirror [--find FIND_OPTIONS...] [--lsf LSF_OPTIONS...] SOURCE TARGET
    imirror --progress [SOURCE]

### Invocation

| Option                   | Description                               |
| :----------------------- | :---------------------------------------- |
| `--find FIND_OPTIONS...` | Optional arguments given to `find`(1), to determine which files will be copied |
| `--lsf LSF_OPTIONS...`   | Optional arguments given to the LSF job submission (`bsub`) |
| `SOURCE`                 | POSIX directory to source                 |
| `TARGET`                 | iRODS collection to target                |

If `imirror` has yet to be run against the `SOURCE` directory, it will
copy all regular files within `SOURCE`, mirroring its structure, to the
`TARGET` collection. Otherwise, see [Tracking](#tracking) (n.b., neither
specificity nor the `TARGET` collection will affect the decision to copy
or report).

Files are selected for upload using `find`(1); this can be further
refined using the `--find` declaration. For example:

    imirror --find -name "*.tar.gz" -o -name "*.tgz" /source/directory /target/collection

The cluster distribution uses the `bsub` defaults, which can be
fine-tuned using the `--lsf` declaration. For example:

    imirror --lsf -G my_project -q long /source/directory /target/collection

**Note** You should not attempt to set the `-J` (job name), `-o`
(standard output logging), `-e` (standard error logging) or `-w` (job
dependency expression) options to `bsub`, as these are used internally.
Setting these in the `--lsf` declaration may cause unindented
consequences.

**Hint** As well as the usual queue, user/group and resource requirement
parameters, the `--lsf` declaration could be used to do iRODS Kerberos
authentication in a pre-execution command (i.e., using the `-E` option
to `bsub`). There is no internal parsing of the declarations (hence the
above warning), so anything goes.

### Tracking

Using the `--progress` option against the `SOURCE` directory (or the
current working directory, if `SOURCE` is not specified) will cause
`imirror` to read the logs, to give a detailed report on its progress.

### Environment Variables

A number of environment variables are available that affect the
operation of `imirror`:

| Environment Variable | Default | Description                         |
| :------------------- | :------ | :---------------------------------- |
| `IPUT_LIMIT`         | 10      | Maximum number of concurrent iRODS uploads |
| `ELEMENTS`           | 200     | The number of chunks into which to distribute the upload |

Tuning these is left as an exercise for the reader.

### Working Directory

When `imirror` is invoked, it will create a working directory (if one
doesn't already exist) in `SOURCE`, named `.imirror`. This directory and
its contents will not be included in the copy. All working files,
including logs, will reside here; it will not be deleted once `imirror`
completes.
