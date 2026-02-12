# Data Ingestor

A lightweight Kafka-based data processing pipeline built with Java. Split into three independent applications for ingesting, processing, and writing data.

## Architecture

```
CSV/Files → [ingest] → Kafka Topic → [process] → Kafka Topic → [write] → JSON/SQL/etc
```

- **ingest**: Reads data from various sources and produces to Kafka
- **process**: Consumes from Kafka, transforms data, produces back to Kafka
- **write**: Consumes from Kafka and writes to various data stores

## Prerequisites

- Java 25 (installed via SDKMAN)
- Kafka cluster (local or remote)
- Gradle 9.3+ (wrapper included)

## Quick Start

### 1. Set Kafka Bootstrap Servers (optional)
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092  # default
```

### 2. Ingest CSV to Kafka
```bash
./gradlew :ingest:run --args="-f data.csv -t raw-events"
```

### 3. Write Kafka to JSON File
```bash
# Read all messages and stop after 5 seconds of idle
./gradlew :write:run --args="-t raw-events -o output.jsonl --idle-timeout 5"

# Or read exactly 100 records
./gradlew :write:run --args="-t raw-events -o output.jsonl -m 100"
```

## Module Details

### Ingest Module

Reads CSV files and produces JSON records to Kafka.

**Usage:**
```bash
./gradlew :ingest:run --args="-f <file> -t <topic>"
```

**Options:**
- `-f, --file`: CSV file path (required)
- `-t, --topic`: Kafka topic to produce to (required)

**Example:**
```bash
./gradlew :ingest:run --args="-f data/sales.csv -t raw-sales"
```

### Write Module

Consumes from Kafka and writes to JSON files (JSONL format).

**Usage:**
```bash
./gradlew :write:run --args="-t <topic> -o <output-file> [options]"
```

**Options:**
- `-t, --topic`: Kafka topic to consume from (required)
- `-o, --output`: Output JSON file path (required)
- `-g, --group`: Consumer group ID (default: `writer-group`)
- `-m, --max-records`: Maximum records to process, then stop (0 = infinite)
- `--idle-timeout`: Stop after X seconds of no new messages (0 = disabled)

**Examples:**
```bash
# Read all available messages
./gradlew :write:run --args="-t raw-events -o output.jsonl --idle-timeout 10"

# Read exactly 1000 records
./gradlew :write:run --args="-t raw-events -o output.jsonl -m 1000"

# Run continuously (Ctrl+C to stop)
./gradlew :write:run --args="-t raw-events -o output.jsonl"
```

### Process Module

(Coming soon - transforms data between Kafka topics)

## Building

### Build all modules
```bash
./gradlew build
```

### Build specific module
```bash
./gradlew :ingest:build
./gradlew :write:build
```

### Create fat JARs for deployment
```bash
./gradlew :ingest:fatJar
./gradlew :write:fatJar

# Output: build/libs/*-fat.jar
```

## Configuration

### Kafka Connection

Set via environment variable:
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

## Development

### Adding a new DataSource

1. Implement `DataSource<T>` interface in `ingest/`
2. Add CLI subcommand or factory logic to instantiate it
3. Pass Kafka producer to the data source

### Adding a new DataSink

1. Implement `DataSink<T>` interface in `write/`
2. Implement `AutoCloseable` for resource cleanup
3. Add CLI options to configure it
