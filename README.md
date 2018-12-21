# imirror

Mirror a directory on a POSIX filesystem into an iRODS collection,
distributing the upload over an LSF cluster.

## Usage

    imirror [--find FIND_OPTIONS...] [--bsub BSUB_OPTIONS...]
            [--iput IPUT_OPTIONS...] SOURCE TARGET

    imirror --progress [SOURCE]

    imirror --resume [SOURCE]

### Invocation

| Option                   | Description                               |
| :----------------------- | :---------------------------------------- |
| `--find FIND_OPTIONS...` | Optional arguments given to `find`(1), to determine which files will be copied |
| `--bsub BSUB_OPTIONS...` | Optional arguments given to the LSF job submission (`bsub`) |
| `--iput IPUT_OPTIONS...` | Optional arguments given to the iRODS upload (`iput`) |
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
fine-tuned using the `--bsub` declaration. For example:

    imirror --bsub -G my_project -q long /source/directory /target/collection

**Note** You should not attempt to set the `-J` (job name), `-o`
(standard output logging) or `-e` (standard error logging) options to
`bsub`, as these are used internally. Setting these in the `--bsub`
declaration may cause unindented consequences.

**Hint** As well as the usual queue, user/group and resource requirement
parameters, the `--bsub` declaration could be used to do iRODS Kerberos
authentication in a pre-execution command (i.e., using the `-E` option
to `bsub`). There is no internal parsing of the declarations (hence the
above warning), so anything goes.

To fine-tune the behaviour of the iRODS upload (`iput`), you may use the
`--iput` declaration. Note, however, that the default invocation is
already tuned for resilience in the following ways:

* Client and server-side checksum validation;
* Retrying (up to three times), with restart information for small and
  large files;
* Socket keepalive;
* Exclusive write locking.

There are very few options to `iput` that are still relevant (e.g.,
ticket-based access with `-t`), but they can be specified here if needs
be.

### Resumption

Using the `--resume` option against the `SOURCE` directory (or the
current working directory, if `SOURCE` is not specified) will cause
`imirror` to resume a previously aborted upload. If the `SOURCE` has
completed or is still in progress, then the tracking report will be
shown instead.

**Note** If whatever issue that caused the abortion in the first place
has not been resolved in the meantime, there is no reason to suspect
that resuming the upload won't immediately fail again.

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

**Note** `ELEMENTS` should not be set higher than 10,000 as it will
break the way the file manifest is chunked. Your LSF administrator will
have probably complained at you long before this point.

### State

When `imirror` is invoked, it will create a working directory in the
source directory named `.imirror`, accessible only to that user. This
directory and its contents will not be included in the copy. All working
files, including logs, will reside here; it will not be deleted once
`imirror` completes.

#### Manifest

The list of found files is split up into approximately even chunks, of
up to `ELEMENTS` in number. This defines how the upload is distributed
across files. (Note that chunk size is a function of the number of
files, rather than aggregate file size. This can cause imbalance in the
distributed jobs; e.g., if one chunk contains a lot of very large files,
versus a lot of very small ones.)

The same list is also used to derive the directory structure. This is
first mirrored on iRODS as subcollections of the target collection
before the uploading starts.

#### Workflow and Logging

The `--progress` option will parse the logs into a human-readable
report. However, for reference sake, the logs are described herein.

The setup job runs first and writes `setup.log`. If it succeeds, it will
also log the number and size of files to be uploaded, as well as how
many chunks it has been distributed into. The upload jobs will then be
submitted, with its job ID logged.

The upload jobs will then log to `copy.XXXX.log` and `done.XXXX.log`,
where `XXXX` is the index of the distributed job. The `copy.XXXX.log` is
the overall log for the job, whereas `done.XXXX.log` will be an
incremental retelling of the respective manifest file, with additional
metadata for reporting purposes.

When the `--resume` option is correctly used, the state of the aborted
upload will be archived alongside its progress report. The latter will
be used by the `--progress` option to give a full history of the upload.
