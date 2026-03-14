# Kafka 消費者模組實施步驟

> 本模組負責監聽 Kafka 中的 Solana 鏈上資料，並將其寫入 PostgreSQL 資料庫。

---

## 整體資料流

```
Yellowstone gRPC → Kafka Topic → ConsumerController → ConsumerService → PostgreSQL
```

---

## Step 1：安裝依賴套件

```bash
npm install @nestjs/swagger
```

`@nestjs/swagger` 提供 `@ApiProperty()` 裝飾器，用於 Entity 欄位的文件標註。

---

## Step 2：建立實體輔助基類

**檔案：** `src/utils/relational-entity-helper.ts`

所有 TypeORM Entity 的公共基類，提供兩個通用能力：

- `@AfterLoad()` 鉤子：Entity 載入後自動記錄類名至 `__entity`
- `toJSON()`：使用 `class-transformer` 的 `instanceToPlain()` 覆寫序列化行為，支援 `@Exclude`、`@Expose` 等裝飾器

---

## Step 3：建立資料庫 Entity

### 3-1 區塊 Entity

**檔案：** `src/blocks/infrastructure/persistence/relational/entities/block.entity.ts`

| 欄位                         | 型別                   | 說明                                  |
| ---------------------------- | ---------------------- | ------------------------------------- |
| `slot`                       | `string`（DB: bigint） | 主鍵，Solana slot 編號                |
| `blockhash`                  | `string`               | 區塊哈希，唯一標識區塊內容            |
| `block_time`                 | `string`               | Unix 時間戳（秒），轉字串存入         |
| `block_height`               | `string`               | 區塊高度，bigint 轉字串防精度丟失     |
| `parent_slot`                | `number`（DB: bigint） | 父區塊 slot 編號                      |
| `parent_blockhash`           | `string`               | 父區塊哈希                            |
| `rewards`                    | `string`               | 區塊獎勵，JSON.stringify 序列化後存入 |
| `executed_transaction_count` | `string`               | 實際執行交易數                        |
| `entries_count`              | `string`               | PoH entry 數量                        |

### 3-2 交易 Entity

**檔案：** `src/transactions/infrastructure/persistence/relational/entities/transaction.entity.ts`

| 欄位                      | 型別                   | 說明                     |
| ------------------------- | ---------------------- | ------------------------ |
| `id`                      | `string`（UUID）       | 自動生成主鍵             |
| `slot`                    | `string`（DB: bigint） | 交易所在 slot            |
| `signature`               | `string`               | 交易簽名，鏈上唯一標識符 |
| `is_vote`                 | `boolean`              | 是否為驗證者投票交易     |
| `transaction`             | `jsonb`                | 完整交易原始資料         |
| `meta`                    | `jsonb`                | 交易執行結果元數據       |
| `index`                   | `number`               | 交易在 slot 內的排列索引 |
| `createdAt` / `updatedAt` | `Date`                 | TypeORM 自動維護         |

---

## Step 4：定義 Kafka 訊息型別

**檔案：** `src/consumer/domain/types.ts`

定義以下 TypeScript Interface，對應 Yellowstone gRPC 推送的鏈上資料結構：

| Interface     | 說明                                       |
| ------------- | ------------------------------------------ |
| `BlockMeta`   | 區塊元資訊（slot、blockhash、時間戳等）    |
| `Account`     | 帳戶狀態變更（pubkey、餘額、程式擁有者等） |
| `Transaction` | 完整交易（簽名、指令、執行結果）           |
| `Meta`        | 交易執行結果元數據（費用、餘額快照、日誌） |
| `Header`      | 交易訊息頭（簽名帳戶數量分布）             |
| `Instruction` | 單條鏈上指令（程式索引、帳戶、Borsh 資料） |
| `Slot`        | Slot 狀態（自定義，移除 Prisma 依賴）      |

> **注意：** `Slot` 型別原從 `@prisma/client` 匯入，現已遷移至本地 `domain/types.ts`。

---

## Step 5：建立消費者模組

### 5-1 ConsumerModule

**檔案：** `src/consumer/consumer.module.ts`

```
imports:
  - TypeOrmModule.forFeature([BlockEntity, TransactionEntity])  ← 註冊 Repository
  - ClientsModule.register([{ name: 'GEYSER_SERVICE', transport: KAFKA }])  ← 註冊 Kafka 客戶端

controllers: [ConsumerController]
providers:   [ConsumerService]
exports:     [ConsumerService]
```

Kafka 配置：

- Broker：`localhost:9092`
- Consumer Group ID：`solana-indexer`（同組多實例自動負載均衡 partition）

### 5-2 ConsumerController

**檔案：** `src/consumer/consumer.controller.ts`

監聽四個 Kafka Topic，收到訊息後轉發至 ConsumerService：

| Topic                            | 方法               | 說明          |
| -------------------------------- | ------------------ | ------------- |
| `solana.testnet.slot_status`     | `slotStatus()`     | Slot 狀態變更 |
| `solana.testnet.block_metadata`  | `blockMetadata()`  | 區塊元資訊    |
| `solana.testnet.account_updates` | `accountUpdates()` | 帳戶狀態變更  |
| `solana.testnet.transaction`     | `transaction()`    | 完整交易資料  |

> 所有訊息統一包在 `update_oneof` 層（來自 Protobuf `oneof` 語義），Controller 按類型解構後傳入 Service。

### 5-3 ConsumerService

**檔案：** `src/consumer/consumer.service.ts`

原專案使用 PrismaService，現遷移至 TypeORM Repository：

| 方法                | 寫入策略                     | 說明                               |
| ------------------- | ---------------------------- | ---------------------------------- |
| `saveSlot()`        | 僅印日誌（TODO）             | 尚未建立 SlotEntity，待後續擴展    |
| `saveBlock()`       | `insert().orUpdate()` upsert | 以 slot 為主鍵，衝突時更新所有欄位 |
| `saveAccount()`     | 僅印日誌（TODO）             | 尚未建立 AccountEntity，待後續擴展 |
| `saveTransaction()` | `create()` + `save()`        | 簽名天然唯一，直接 INSERT          |

**型別轉換重點（saveBlock）：**

- `slot`：`bigint` → `string`
- `block_time`：`{ timestamp: number }` → 解包後 `.toString()`
- `block_height`：`{ block_height: bigint }` → 解包後 `.toString()`
- `rewards`：`any[]` → `JSON.stringify()`
- `parent_slot`：`bigint` → `Number()`

---

## Step 6：啟動開發伺服器

```bash
cd ~/Chain_Indexer/backend
npm run start:dev
```

若啟動成功，終端機應顯示 `Nest application successfully started`。
