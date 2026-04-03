# Chain Indexer

A high-performance, real-time blockchain data indexing system that transforms raw on-chain data into structured, queryable information. Currently focused on the Solana network, it leverages Helius gRPC/WebSocket, Apache Kafka, NestJS, and PostgreSQL to build a robust, decoupled data pipeline.

## Overview

**The Problem**

Blockchain data exists on-chain but is not inherently queryable. Developers face challenges:
- **Inaccessibility**: Data requires re-scanning the entire blockchain for queries.
- **Latency**: Real-time data access is slow and computationally expensive.
- **Scalability**: High-frequency data streams (like Solana's) require robust, distributed processing.

**Our Solution**

Chain Indexer automatically:
1. Monitors the Solana network (Devnet/Mainnet) via Helius gRPC/WebSocket in real-time.
2. Captures raw block, transaction, and account state data.
3. Streams processed data to Apache Kafka for decoupled, high-throughput consumption.
4. Consumes Kafka messages via a NestJS microservice.
5. Persists full-fidelity JSONB data into a PostgreSQL database, enabling complex SQL queries on raw on-chain data.

## Architecture

```text
Solana Network (Devnet/Mainnet)
          ↓ (Helius gRPC / WebSocket)
    Solana Listener (Producer)
    (Captures Slots, Blocks, Txs, Accounts)
          ↓
    Kafka Producer
          ↓
Apache Kafka Message Queue
├─ solana.testnet.slot_status
├─ solana.testnet.block_metadata
├─ solana.testnet.account_updates
└─ solana.testnet.transaction
          ↓
    NestJS Consumer (Microservice)
    (Subscribes to Kafka Topics)
          ↓
    TypeORM (Data Persistence)
          ↓
PostgreSQL Database
(Stores full JSONB data for complex queries)
```

### Key Architecture Principles

**Decoupled Design**: The Helius listener (Producer) and the NestJS database writer (Consumer) are completely independent, connected only by Kafka. A database slowdown won't block the real-time listener.

**Stream-based Processing**: Event-driven architecture supports Solana's high-frequency data streams with millisecond-level latency.

**Full-Fidelity Storage**: Transactions and metadata are stored as `jsonb` in PostgreSQL, preserving the original on-chain data structure while allowing deep SQL queries (e.g., querying specific account keys within a transaction).

## Tech Stack

- **Framework**: NestJS (Node.js) with TypeScript for robust backend architecture.
- **Blockchain Data**: Helius SDK (gRPC/WebSocket) for real-time Solana data streaming.
- **Message Queue**: Apache Kafka (via Docker) for distributed data streaming.
- **Database**: PostgreSQL with TypeORM for relational and JSONB data storage.
- **Infrastructure**: Docker & Docker Compose for Kafka, Zookeeper, and PostgreSQL environments.

## Getting Started

### Prerequisites
- Node.js 18+
- Docker and Docker Compose (for Kafka and PostgreSQL)
- A Helius API Key (for Solana RPC/gRPC access)

### Installation

```bash
git clone https://github.com/wls503pl/Chain_Indexer.git
cd Chain_Indexer/backend

npm install
```

### Environment Setup

1. Start the infrastructure (Kafka, Zookeeper, PostgreSQL) using Docker:
   *(Ensure you have a `docker-compose.yml` configured for these services)*
   ```bash
   docker-compose up -d
   ```

2. Create a `.env` file in the `backend` directory:

   ```env
   # Database Configuration
   DATABASE_HOST=localhost
   DATABASE_PORT=5432
   DATABASE_USER=postgres
   DATABASE_PASSWORD=postgres
   DATABASE_NAME=chain_indexer

   # Kafka Configuration
   KAFKA_BROKER=localhost:9092

   # Helius RPC (Solana)
   HELIUS_API_KEY=your_helius_api_key_here
   ```

### Running the Application

The application runs as a unified NestJS app that starts both the HTTP server and the Kafka microservice consumer.

```bash
# Development mode with hot reload
npm run start:dev

# Production build
npm run build
npm run start:prod
```

## Database Query Examples

Because we store transaction data as `jsonb`, you can perform powerful queries directly in PostgreSQL.

**1. View recent transactions (Signature and Slot):**
```sql
SELECT signature, slot, is_vote FROM transaction ORDER BY "createdAt" DESC LIMIT 5;
```

**2. Extract specific data from the JSON payload (e.g., Fee Payer and Fee Paid):**
```sql
SELECT 
  signature, 
  transaction->'message'->'accountKeys'->0 as fee_payer, 
  meta->'fee' as fee_paid 
FROM transaction 
WHERE transaction IS NOT NULL 
LIMIT 3;
```

## Project Structure

```text
Chain_Indexer/
└── backend/
    ├── src/
    │   ├── listener/          # Helius gRPC/WS listener (Kafka Producer)
    │   ├── consumer/          # NestJS Kafka Consumer microservice
    │   ├── transactions/      # Transaction entity and database logic
    │   ├── blocks/            # Block entity and database logic
    │   ├── utils/             # Helper functions
    │   ├── app.module.ts      # Main application module
    │   └── main.ts            # Entry point (Starts HTTP & Microservices)
    ├── .env                   # Environment variables
    ├── package.json
    └── tsconfig.json
```

## Current Status & Roadmap

**Currently Implemented:**
- ✅ Real-time Solana Devnet listening via Helius.
- ✅ Kafka producer and topic routing.
- ✅ NestJS Kafka consumer microservice.
- ✅ PostgreSQL persistence with TypeORM and JSONB support.
- ✅ Idempotent database writes (Unique index on transaction signatures).
