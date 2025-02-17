# Running

To deploy the system we provide a `serverless.yml` configuration. Each of the functions listed in the YAML must be compiled with `go build` and compressed in a deployment zip package (see https://docs.aws.amazon.com/lambda/latest/dg/golang-package.html), the `build.sh` script does this process for the handlers.


## Preparation

To prepare the data for the experiments run the `binaries/loader.go` tool.

## Running tests

Tests follow the standard go test structure https://go.dev/doc/tutorial/add-a-test and additional tools for benchmarking the system are under the `benchmark/` directory

