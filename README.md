## `kage` | 影
**Kage** (Shadow) is a high-performance, distributed event-streaming platform built from the ground up in **Go**. Inspired by the Apache Kafka wire protocol, Kage is designed to be a lightweight, single-binary alternative for edge computing environments and high-throughput data pipelines.
Built as the core messaging backbone for the **Kanto** ecosystem.

### 🎯 Vision
The goal of Kage is to provide a Kafka-compatible broker that eliminates the operational complexity of JVM-based systems. While Apache Kafka is a beast for the enterprise, **Kage** focuses on:
- **Zero Dependencies**: No Zookeeper, no JVM. A single Go binary.
- **Predictable Performance**: Utilizing Go's memory model and `mmap` for zero-copy disk I/O.
- **Edge-First**: Optimized for low-resource environments (like Kanto nodes) without sacrificing durability.

### 🏗️ Technical Architecture
Kage implements the **Kafka Wire Protocol**, allowing existing Kafka clients (Java, Go, Python, TypeScript) to communicate with it.

#### Core Components:
- **The Log Engine**: A segmented, append-only log structure. Data is persisted in immutable segments with sparse indexing for $O(\log N)$ lookups.
- **Concurrency Model**: A specialized worker-pool pattern using Goroutines to handle thousands of concurrent TCP connections with minimal overhead.
- **Zero-Copy Path**: Uses `sendfile(2)` and `mmap` to transfer data from the filesystem directly to the network stack, bypassing user-space buffers.

### 🛠️ Getting Started
Prerequisites
Go 1.21+
Installation
```Bash
git clone https://github.com/tu-usuario/kage.git
cd kage
go build -o kaged ./cmd/kaged
```
Running the Broker
```Bash
./kaged --port 9092 --path /tmp/kage-logs
```
### 🤝 Contributing
Kage is an open-source project designed for the Kanto ecosystem. Contributions regarding performance optimization, protocol compatibility, and testing are welcome.

Developed by @codexorange (Senior Software Engineer)