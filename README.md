# Chain Indexer

A high-performance, real-time blockchain data indexing system that transforms raw on-chain data into structured, queryable information using Node.js, TypeScript, and Apache Kafka.

## Overview

**The Problem**

Blockchain data exists on-chain but is not inherently queryable. Developers face challenges:
- **Inaccessibility**: Data requires re-scanning the entire blockchain for queries
- **Latency**: Real-time data access is slow and computationally expensive
- **Inconsistency**: Different blockchains have different data formats and structures
- **Scalability**: High-frequency data streams require robust, distributed processing

**Our Solution**

Chain Indexer automatically:
1. Monitors blockchain nodes via RPC in real-time
2. Parses and validates raw block/transaction data
3. Standardizes data across multiple chains
4. Streams processed data to Apache Kafka for decoupled consumption
5. Enables downstream applications to access blockchain data without direct chain queries

## Architecture

```
Blockchain Network (Ethereum, Polygon, Solana, etc.)
          ↓ (JSON-RPC)
    Block Listener
    (WebSocket/Polling)
          ↓
    Data Parser
    (Extract & Validate)
          ↓
    Data Transformer
    (Standardize Format)
          ↓
    Kafka Producer
          ↓
Apache Kafka Message Queue
├─ chain-blocks (Block data)
├─ chain-transactions (Transaction details)
├─ chain-events (Smart contract events)
└─ chain-states (State changes)
          ↓
    Multiple Consumers
    ├─ Database (PostgreSQL/ClickHouse)
    ├─ Analytics System
    └─ Real-time API Gateway
```

### Key Architecture Principles

**Decoupled Design**: Producers and consumers are completely independent. A consumer failure doesn't impact producers or other consumers.

**Stream-based Processing**: Event-driven architecture supports high-frequency data streams with millisecond-level latency.

**Multi-chain Support**: Unified data format with chain-specific metadata enables seamless cross-chain compatibility.

## Components

### Block Listener
- Real-time blockchain node monitoring
- Dual-mode operation: WebSocket for efficiency, polling for compatibility
- Automatic reorg detection and handling
- Connection pool management and retry logic

### Data Parser
- Extracts block metadata (height, timestamp, miner, hash)
- Parses transaction details (from, to, value, gas, input data)
- Decodes smart contract event logs
- Validates data integrity and completeness

### Data Transformer
- Converts raw blockchain data to standardized JSON structure
- Normalizes data across different blockchain networks
- Enriches data with computed fields (e.g., ETH conversions, gas costs)
- Applies custom transformation rules per chain

### Kafka Producer
- High-throughput message publishing to Kafka brokers
- Topic-based routing (blocks → chain-blocks, transactions → chain-transactions, etc.)
- Batch optimization for performance
- Error handling and retry mechanisms

## Tech Stack

- **Runtime**: Node.js with async/await for high concurrency
- **Language**: TypeScript for type safety and maintainability
- **Blockchain**: Web3.js for RPC communication
- **Message Queue**: Apache Kafka for distributed data streaming
- **Testing**: Jest/Vitest for unit and integration tests
- **CI/CD**: GitHub Actions for automated build and lint workflows

## Use Cases

### 1. DeFi Trading Bots
Subscribe to transaction stream for real-time trade execution and arbitrage detection.

### 2. Blockchain Analytics Platforms
Accumulate indexed data in a database for SQL-based analysis and reporting.

### 3. Real-time Monitoring Dashboards
Connect WebSocket consumers to display live transaction activity and network metrics.

### 4. Event-driven Applications
Monitor smart contract events and trigger automated workflows.

### 5. Data Lake Architecture
Archive indexed data for historical analysis and backtest scenarios.

## Getting Started

### Prerequisites
- Node.js 16+ (for native async/await support)
- Apache Kafka 2.8+ (standalone or managed)
- Access to blockchain RPC endpoint (Infura, Alchemy, or local node)

### Installation

```bash
git clone https://github.com/wls503pl/Chain_Indexer.git
cd Chain_Indexer

npm install
```

### Configuration

Create `.env` file:

```
# Blockchain RPC
RPC_ENDPOINT=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
RPC_POLL_INTERVAL=12000  # Poll every 12 seconds

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=chain-indexer-1

# Indexer
CHAIN_ID=1  # Ethereum mainnet
BLOCK_CONFIRMATION=12  # Wait for 12 block confirmations
```

### Running

```bash
# Development mode with hot reload
npm run dev

# Production build
npm run build
npm run start

# Run tests
npm test
```

## API Usage

### Block Topic Consumer Example

```typescript
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'block-analyzer' });

await consumer.connect();
await consumer.subscribe({ topic: 'chain-blocks' });

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const block = JSON.parse(message.value.toString());
    console.log(`Block #${block.number} with ${block.transactions.length} txs`);
  },
});
```

### Message Format

**chain-blocks topic:**
```json
{
  "number": 19500000,
  "hash": "0xabcd...",
  "timestamp": 1710000000,
  "miner": "0xF34d....",
  "transactions": 150,
  "gasUsed": "29999999",
  "chainId": 1
}
```

**chain-transactions topic:**
```json
{
  "hash": "0x1234...",
  "blockNumber": 19500000,
  "from": "0xabcd...",
  "to": "0xef01...",
  "value": "1000000000000000000",
  "gasPrice": "25000000000",
  "input": "0xa9059cbb...",
  "status": 1,
  "chainId": 1
}
```

## Performance

- **Latency**: 2-5 second delay from blockchain to Kafka (depending on RPC provider)
- **Throughput**: Handles 1000+ transactions per second
- **Scalability**: Horizontal scaling via Kafka partitions and consumer groups
- **Reliability**: Guaranteed at-least-once delivery with Kafka

## Project Structure

```
Chain_Indexer/
├── src/
│   ├── listener/          # Block listener implementation
│   ├── parser/            # Data parsing logic
│   ├── transformer/       # Data transformation rules
│   ├── kafka/             # Kafka producer/consumer setup
│   ├── types/             # TypeScript interfaces
│   └── index.ts           # Entry point
├── tests/
│   ├── unit/
│   └── integration/
├── .github/workflows/     # CI/CD pipelines
├── .env.example
├── README.md
├── package.json
└── tsconfig.json
```

## Development

### Running Tests

```bash
npm test                 # Run all tests
npm run test:watch     # Watch mode
npm run test:coverage  # Coverage report
```

### Code Quality

```bash
npm run lint           # ESLint
npm run format         # Prettier
npm run type-check     # TypeScript check
```

## Multi-chain Support

Currently supported:
- Ethereum (Mainnet, Sepolia, Goerli)
- Polygon (Mainnet, Mumbai)
- Arbitrum (Mainnet, Sepolia)
- Optimism (Mainnet, Sepolia)
- Solana (Mainnet, Devnet) — via Solana RPC
- Additional EVM chains via configuration

Add new chains by extending the configuration:

```typescript
const chainConfig = {
  ethereum: { rpcUrl: '...', chainId: 1 },
  polygon: { rpcUrl: '...', chainId: 137 },
  // Add more chains here
};
```

## Monitoring & Observability

Built-in metrics:
- **Processed blocks**: Counter of blocks parsed
- **Processing latency**: Time from block creation to Kafka publish
- **Error rate**: Failed parsing attempts
- **Kafka lag**: Consumer lag in message queues

Integration with:
- Prometheus for metrics collection
- Grafana for visualization
- ELK Stack for log aggregation

## Limitations & Future Work

### Current Limitations
- ⚠️ Requires running local Kafka cluster (managed services coming soon)
- ⚠️ Single-machine deployment (horizontal scaling pending)
- ⚠️ Block reorg handling covers up to 12 blocks deep (EIP-3675 compatible)

### Roadmap
- [ ] Managed Kafka cloud integration (Confluent Cloud, AWS MSK)
- [ ] State snapshot capability for faster consumer catchup
- [ ] GraphQL API for historical data queries
- [ ] Neon database integration for serverless deployment
- [ ] Docker Compose setup for local development
- [ ] Metrics dashboard (Grafana templates)
- [ ] Web3 domain support (ENS resolution)
- [ ] Gas optimization tracking

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit changes (`git commit -m 'Add feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## Acknowledgments

- Built with [Web3.js](https://web3js.readthedocs.io/) for blockchain interaction
- Powered by [Apache Kafka](https://kafka.apache.org/) for streaming
- Inspired by production indexing systems used in DeFi protocols
- 
---

**Chain Indexer** — Making blockchain data accessible, in real-time, at scale.
