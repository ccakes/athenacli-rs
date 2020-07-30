# athenacli

This is a simple CLI tool for executing queries against AWS Athena. This scratches a personal itch as the AWS CLI is cumbersome to work with for this use case and the alternatives seem predominantly to be in scripting languages that require more setup than I'd like.

This is simple to include in scripts and can run queries either passed in via an argument or from a file and is a statically-compiled binary for extra simple deployments.

### Getting Started

Head over to the [Releases](https://github.com/ccakes/athenacli-rs/releases) page to download a binary for Linux or macOS.

### Usage

```
$ athenacli --help
athenacli 0.2.0
Basic Athena CLI

USAGE:
    athenacli [FLAGS] [OPTIONS] --database <database> --region <region> --results <result-bucket>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -v               Logging verbosity (repeat for more detail)

OPTIONS:
    -c, --command <command>...       run a single SQL statement, can be repeated
    -d, --database <database>        database name to connect to
    -f, --file <file>                execute one or more SQL statements from a file, then exit
    -r, --region <region>            AWS region [env: AWS_REGION=]
    -b, --results <result-bucket>    S3 bucket name for results (eg s3://my-results)
    -w, --workgroup <workgroup>      Athena workgroup to use
```

### Authentication

This uses the standard methods for discovering AWS credentials, it'll check the environment, `~/.aws/config` and look for EC2 metadata (I _think_ in that order..)

### Contributions

Contributions are welcome - feel free to submit a PR!

### License

MIT