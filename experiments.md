(AI Generated)

## Documentation for the `loader` Benchmarking Tool

### 1. Overview

The `loader` tool is a command-line utility for running performance and latency benchmarks. It is designed to compare a **"System Under Test" (SUT)** against a **"Baseline"** implementation. The benchmarks are run in different application domains, identified as **"Hotel Reservation"** and **"Banking"**.

The tool operates through a series of commands that manage the different phases of an experiment: setting up initial state, loading a workload (messages/tasks), and executing the workload with a configurable number of workers.

### 2. Directory Structure

The tool expects a specific directory structure to be in place:

```
.
├── loader          # The executable binary
├── params/         # Directory for all JSON configuration files
└── time-logs/      # Directory where all output logs are written
```

### 3. Core Concepts

*   **SUT vs. Baseline**: Experiments are run against either the `sut` (the new system being evaluated) or a `baseline` (a control or existing system for comparison).
*   **Domains**: The tool is pre-configured for at least two application domains: `Hotel` (Hotel Reservation) and `Banking`.
*   **Experiment Phases**: A typical experiment consists of several distinct phases:
    1.  **Load State**: Initializes the system with a starting dataset (e.g., creating accounts, populating hotel data).
    2.  **Load/Send Messages**: Prepares or sends a queue of tasks/requests that will be executed during the test.
    3.  **Assign Tasks**: (Optional) A step to distribute the prepared tasks among workers.
    4.  **Run Workers / Start Benchmark**: Kicks off the actual test, where multiple worker processes execute the tasks and record performance metrics.
*   **Configuration**: All aspects of the experiments are controlled by JSON files in the `params/` directory and occasionally by environment variables.
*   **Output**: Results from each run are stored in a uniquely named subdirectory within `time-logs/`. The directory name is descriptive of the experiment's parameters.

### 4. Command Reference

The `loader` tool is invoked using the following syntax:

`./loader [environment] <command>`

*   **`[environment]`**: This is almost always `aws`, suggesting the tool is designed to interact with an AWS environment. It might be optional for local commands like `timeServer`.
*   **`<command>`**: The specific action to perform.

#### Common Commands

**Setup & Utility:**

*   `timeServer`: Starts a time synchronization server. This appears to be a prerequisite background process for experiments and should be run in a separate session (e.g., using `screen`).
    *   Example: `./loader aws timeServer`

**Experiment Workflow Commands:**

Commands follow a `(sut|baseline)(Domain)(Action)` naming convention.

*   `sutHotelLoadState` / `sutBankingLoadState`: Loads the initial state for the SUT in the specified domain.
*   `baselineHotelLoadState` / `baselineBankingLoadState`: Loads the initial state for the Baseline.

*   `sutHotelLoadMessages` / `sutBankingLoadMessages`: Loads the workload messages/tasks for the SUT.
*   `baselineHotelSendMessages` / `baselineBankingSendMessages`: Loads and immediately begins sending workload messages for the Baseline. This appears to be a combined "load and run" step for the baseline tests.

*   `randomlyAssignTasks`: An optional step after `LoadMessages` to distribute the workload.
    *   Example: `./loader aws randomlyAssignTasks`

*   `sutRunWorkers`: Starts the SUT workers to process the loaded messages. This is the main throughput benchmark execution command.
    *   Example: `./loader aws sutRunWorkers`

*   `sutHotelStartLatencyBenchmark` / `sutBankingStartLatencyBenchmark`: Starts a specific latency-focused benchmark for the SUT. This seems to be an alternative to the `sutRunWorkers` command for measuring response times under specific conditions.

### 5. Configuration

Experiments are configured via JSON files in the `params/` directory and environment variables.

#### Parameter Files (`params/`)

*   `run-specific-params.json`: Contains parameters for a single experiment run, such as the number of concurrent actors, duration, and the name of the output log directory. This file is edited frequently before executing a run.
*   `sut-run-workers.json`: Configures the SUT worker environment, likely specifying worker IPs, the number of worker processes, and polling intervals.
*   `sut-[domain]-params.json` (e.g., `sut-banking-params.json`): Defines parameters for the SUT application itself, including state loading parameters (e.g., number of accounts/hotels to create).
*   `baseline-[domain]-state-params.json` (e.g., `baseline-hotel-reservation-state-params.json`): Configures the initial state for the Baseline experiment.
*   `baseline-[domain]-requests-params.json` (e.g., `baseline-banking-requests-params.json`): Defines the workload for the Baseline experiment (e.g., number and type of transactions).

#### Environment Variables

*   `CONCURRENT_LOADING_UNITS`: Controls the level of parallelism for data loading operations (`LoadState`, `LoadMessages`).
    *   Example: `export CONCURRENT_LOADING_UNITS=10`

### 6. How to Run an Experiment (Inferred Workflow)

#### A. SUT Throughput Benchmark Workflow

1.  **Start Background Services**: In a `screen` session, start the time server.
    ```bash
    screen
    ./loader aws timeServer
    # Detach from screen (Ctrl+A, D)
    ```

2.  **Configure and Load Initial State**:
    *   Edit `params/sut-[domain]-params.json` to define the initial state (e.g., number of hotels/users).
    *   Run the state loader.
    ```bash
    vim params/sut-hotel-reservation-params.json
    ./loader aws sutHotelLoadState
    ```

3.  **Configure and Load Workload**:
    *   Edit relevant parameter files to define the workload (e.g., number of transactions).
    *   Run the message loader.
    ```bash
    vim params/sut-hotel-reservation-params.json
    ./loader aws sutHotelLoadMessages
    ```

4.  **(Optional) Assign Tasks**: If the workload needs to be distributed, run the assignment command.
    ```bash
    ./loader aws randomlyAssignTasks
    ```

5.  **Configure and Run the Benchmark**:
    *   Edit `params/run-specific-params.json` to set the output directory name, test duration, etc.
    *   Edit `params/sut-run-workers.json` to configure the number of workers.
    *   Execute the benchmark.
    ```bash
    vim params/run-specific-params.json
    vim params/sut-run-workers.json
    ./loader aws sutRunWorkers
    ```

6.  **Analyze Results**:
    *   Navigate to the `time-logs/` directory to find the output logs in a newly created subdirectory.
    *   Inspect the log files (`0.log`, `1.log`, etc.), which likely correspond to individual workers.
    ```bash
    cd time-logs/SUT_HOTEL_.../
    wc -l *.log
    head 0.log
    ```

#### B. Baseline Benchmark Workflow

The Baseline workflow is more streamlined, combining the message sending and execution steps.

1.  **Start Background Services** (Same as SUT).

2.  **Configure and Load Initial State**:
    ```bash
    vim params/baseline-banking-state-params.json
    ./loader aws baselineBankingLoadState
    ```

3.  **Configure and Run the Benchmark**:
    *   Edit `params/run-specific-params.json` and `params/baseline-banking-requests-params.json` to define the workload and run parameters.
    *   Run the combined send/execute command.
    ```bash
    vim params/run-specific-params.json
    vim params/baseline-banking-requests-params.json
    ./loader aws baselineBankingSendMessages
    ```

4.  **Analyze Results** (Same as SUT).