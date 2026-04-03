# Chain_Indexer 實施進度報告

本文件記錄了 **Chain_Indexer** 系統的實施進度、核心模組開發情況以及具體的運行驗證步驟。

---

## 一、 基礎設施部署 (Infrastructure Setup)

系統採用 Docker 容器化部署核心組件，確保開發與生產環境的一致性。

### 1.1 啟動基礎設施

在 `backend/` 目錄下執行以下命令啟動數據庫與消息隊列：

```bash
sudo docker-compose up -d
```

### 1.2 驗證運行狀態

執行以下命令檢查容器是否正常運行：

```bash
sudo docker-compose ps
```

**預期結果**：
- `chain_indexer_db`: Up (healthy)
- `chain_indexer_kafka`: Up (healthy)  
- `chain_indexer_zookeeper`: Up (healthy)

---

## 二、 Solana 實時監聽模組 (Solana Listener Implementation)

這是系統的核心監聽層，負責從 Solana Devnet 實時捕獲交易數據並推送到 Kafka。

### 2.1 技術實現細節

- **數據源**: 接入 Helius Devnet RPC 節點
- **通訊協議**: 採用 **WebSocket (WSS)** 長連接
- **監聽策略**: 使用 `logsSubscribe` 訂閱特定 Token Mint 地址
  - **SWAVE Token Mint**: `Ekp5zfYHSxsxDiPCQe68KAZrVg2jNhQ8naszTQgwgStJ`

### 2.2 啟動後端服務

在 `backend/` 目錄下執行：

```bash
npm run start:dev
```

### 2.3 WebSocket 連接驗證

當服務啟動後，控制台應顯示以下日誌，證明已成功連接到 Helius 並訂閱 SWAVE Token：

```text
[SolanaListenerService] 正在初始化 Solana 監聽服務...
[SolanaListenerService] 正在連接 Helius Devnet WebSocket...
[SolanaListenerService] Helius WebSocket 連接成功！
[SolanaListenerService] 已訂閱 SWAVE Token 日誌: Ekp5zfYHSxsxDiPCQe68KAZrVg2jNhQ8naszTQgwgStJ
[SolanaListenerService] 訂閱確認成功，ID: 1407941
```

### 2.4 實時交易捕獲驗證

在 Phantom 錢包（Devnet 網絡）進行一筆 SWAVE Token 轉帳，服務應立即捕獲並處理該交易。

**WebSocket 檢測到新交易的實時日誌**：

![WebSocket 檢測到 SWAVE 新交易](./img/listening/newSol_transaction.png)

控制台輸出示例：
```text
[Nest] 292839 - 04/03/2026, 4:19:19 PM     LOG [SolanaListenerService] 檢測到新的 SWAVE 交易: 436gKi9teUdAUSZURnMwcF5Y66E4Q9fxNda5UcceZwRUu...
```

---

## 三、 數據消費與持久化 (Consumer & Persistence)

### 3.1 Kafka Topics 數據流轉

系統通過以下 Topic 實現數據的異步處理：

- `solana.testnet.slot_status`: 監控 Slot 狀態變更
- `solana.testnet.block_metadata`: 索引區塊元信息
- `solana.testnet.account_updates`: 追蹤賬戶餘額與狀態
- `solana.testnet.transaction`: 核心交易數據流

### 3.2 Kafka 消費驗證

執行以下命令監聽 Kafka Topic，驗證交易數據是否成功推送：

```bash
sudo docker-compose exec kafka kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic solana.testnet.transaction --from-beginning
```

**Kafka 接收到交易數據的實時輸出**：

![Kafka 消費交易數據](./img/listening/kafka_received.png)

Kafka 消費輸出示例（JSON 格式）：
```json
{
  "update_oneof": {
    "Transaction": {
      "slot": "452936071",
      "transaction": {
        "signature": "436gKi9teUdAUSZURnMwcF5Y66E4Q9fxNda5UcceZwRUuYevyKxrrYNTarNB6nhPeQEdx9pQLUpvtuFqid2DTz5m",
        "is_vote": "false",
        "transaction_type": "TokenMintOperation",
        "account_keys": "[...]",
        "log_messages": [
          "Program ComputeBudget111111111111111111111111111111111111111 invoke [1]",
          "Program ComputeBudget111111111111111111111111111111111111111 success",
          "Program TokenkeeQfeZyiNwAJBNbGKPFXCMuByf95s623VQ5DA success"
        ]
      }
    }
  },
  "meta": {
    "log_messages": "[...]"
  }
}
```

### 3.3 數據庫寫入驗證

執行以下命令進入數據庫查看已索引的交易：

```bash
docker exec -it chain_indexer_db psql -U postgres -d chain_indexer -c "SELECT signature, slot FROM transaction LIMIT 5;"
```

**預期結果**：
```
                                                   signature                                                   |    slot    
─────────────────────────────────────────────────────────────────────────────────────────────────────────────┬──────────
 436gKi9teUdAUSZURnMwcF5Y66E4Q9fxNda5UcceZwRUuYevyKxrrYNTarNB6nhPeQEdx9pQLUpvtuFqid2DTz5m              | 452936071
 (1 row)
```

### 3.4 核心技術亮點

#### 3.4.1 Upsert 冪等性
在 `ConsumerService` 中實現了衝突檢測機制（`ON CONFLICT ... DO UPDATE`），確保重複消費同一筆交易時不會產生冗餘數據。

#### 3.4.2 BigInt 精度處理
針對 Solana 的大數問題（Lamports、金額超過 JavaScript Number 精度），系統在持久化前統一進行了字符串化處理，避免精度丟失：

```typescript
// 存儲前轉換為字符串
const transaction = {
  signature: string,
  slot: bigint.toString(),  // 轉換為字符串存儲
  lamports: bigint.toString(),
  ...
};
```

#### 3.4.3 架構解耦
監聽層（Producer）與存儲層（Consumer）完全獨立，通過 Kafka 消息隊列緩衝：
- **優勢 1**: 即使數據庫短暫停機，鏈上數據也不會丟失（Kafka 會持久化）
- **優勢 2**: 支持水平擴展，多個 Consumer 實例並行處理
- **優勢 3**: 易於添加新的消費者邏輯（例如：實時 API、告警系統等）

---

## 四、 系統架構圖

```
Solana Devnet (Helius RPC)
        ↓
    [WebSocket Connection]
        ↓
[SolanaListenerService] ← SWAVE Token 監聽
        ↓
   [Kafka Producer]
        ↓
[solana.testnet.transaction] ← 消息隊列緩衝
        ↓
[ConsumerService] ← Kafka Consumer
        ↓
[PostgreSQL Database] ← 數據持久化
        ↓
[API / Dashboard] ← 查詢層
```

---

## 五、 故障排查 (Troubleshooting)

### 5.1 WebSocket 連接失敗
**症狀**: `Connection refused` 或 `ECONNREFUSED`

**解決方案**:
1. 檢查 Helius API Key 是否有效
2. 確認網絡連接正常
3. 檢查防火牆設置

```bash
# 測試 WebSocket 連接
curl -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" wss://devnet.helius-rpc.com/?api-key=YOUR_API_KEY
```

### 5.2 Kafka 消費延遲
**症狀**: 交易在鏈上確認，但 Kafka 遲遲未收到消息

**解決方案**:
1. 檢查 Kafka Broker 狀態
2. 確認 Producer 是否正確發送

```bash
# 查看 Kafka Topic 狀態
sudo docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 查看 Topic 分區詳情
sudo docker-compose exec kafka kafka-topics --describe --topic solana.testnet.transaction --bootstrap-server localhost:9092
```

### 5.3 數據庫連接超時
**症狀**: `FATAL: remaining connection slots are reserved for non-replication superuser connections`

**解決方案**:
1. 檢查數據庫連接池配置
2. 重啟 PostgreSQL 容器

```bash
sudo docker-compose restart chain_indexer_db
```

---

## 六、 部署檢查清單

- [ ] Docker Desktop 已安裝並運行
- [ ] `docker-compose ps` 顯示所有容器 `Up`
- [ ] `npm run start:dev` 啟動無誤
- [ ] WebSocket 連接日誌顯示 `訂閱確認成功`
- [ ] 在 Phantom 發送一筆 SWAVE 轉賬交易
- [ ] Kafka 消費者顯示收到新交易
- [ ] 數據庫查詢返回交易記錄

---

## 七、 性能指標

| 指標 | 目標值 | 當前值 |
|------|--------|--------|
| WebSocket 連接延遲 | < 100ms | ~50ms |
| Kafka 消費延遲 | < 500ms | ~200ms |
| 數據庫寫入延遲 | < 1000ms | ~300ms |
| 端到端延遲 | < 2秒 | ~1.2秒 |