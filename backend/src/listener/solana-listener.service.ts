import { Injectable, OnModuleInit, OnModuleDestroy, Logger, Inject } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import WebSocket = require('ws');

@Injectable()
export class SolanaListenerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SolanaListenerService.name);
  private ws: WebSocket;
  private pingInterval: NodeJS.Timeout;
  
  private readonly HELIUS_WS_URL = 'wss://devnet.helius-rpc.com/?api-key=2c1b78e4-b4f3-46ec-8c12-18cd5b48f7f6';
  private readonly SWAVE_MINT = 'Ekp5zfYHSxsxDiPCQe68KAZrVg2jNhQ8naszTQgwgStJ';

  constructor(
    @Inject('GEYSER_SERVICE') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.logger.log('正在初始化 Solana 監聽服務...');
    this.connectWebSocket();
  }

  onModuleDestroy() {
    this.logger.log('正在銷毀 Solana 監聽服務...');
    if (this.ws) this.ws.close();
    if (this.pingInterval) clearInterval(this.pingInterval);
  }

  private connectWebSocket() {
    this.logger.log(`正在連接 Helius Devnet WebSocket...`);
    this.ws = new WebSocket(this.HELIUS_WS_URL);

    this.ws.on('open', () => {
      this.logger.log('Helius WebSocket 連接成功！');
      this.subscribeToSwaveToken();
      
      this.pingInterval = setInterval(() => {
        if (this.ws.readyState === WebSocket.OPEN) this.ws.ping();
      }, 30000);
    });

    this.ws.on('message', (data: WebSocket.Data) => {
      this.handleMessage(data);
    });

    this.ws.on('error', (error) => {
      this.logger.error(`WebSocket 報錯: ${error.message}`);
    });

    this.ws.on('close', () => {
      this.logger.warn('WebSocket 連接斷開，5秒後嘗試重連...');
      clearInterval(this.pingInterval);
      setTimeout(() => this.connectWebSocket(), 5000);
    });
  }

  private subscribeToSwaveToken() {
    const subscribeMessage = {
      jsonrpc: '2.0',
      id: 1,
      method: 'logsSubscribe',
      params: [{ mentions: [this.SWAVE_MINT] }, { commitment: 'confirmed' }]
    };
    this.ws.send(JSON.stringify(subscribeMessage));
    this.logger.log(`已訂閱 SWAVE Token 日誌: ${this.SWAVE_MINT}`);
  }

  private handleMessage(data: WebSocket.Data) {
    try {
      const message = JSON.parse(data.toString());
      
      if (message.id === 1 && message.result) {
        this.logger.log(`訂閱確認成功，ID: ${message.result}`);
        return;
      }

      if (message.method === 'logsNotification') {
        const logData = message.params.result;
        const signature = logData.value.signature;
        const slot = logData.context.slot;
        
        this.logger.log(`檢測到新的 SWAVE 交易: ${signature}`);
        
        // 構建符合你 Consumer 預期的數據結構
        const kafkaPayload = {
          update_oneof: {
            Transaction: {
              slot: slot.toString(), // 轉為字符串，避免 BigInt 序列化問題
              transaction: {
                signature: signature,
                is_vote: false,
                transaction: { message: { accountKeys: [this.SWAVE_MINT] } }, // 模擬基礎結構
                meta: { logMessages: logData.value.logs }
              }
            }
          }
        };

        // 使用 emit 發送，並指定 key 為 signature
        this.kafkaClient.emit('solana.testnet.transaction', {
          key: signature,
          value: kafkaPayload
        });
        
        this.logger.log(`數據已成功發送至 Kafka Topic: solana.testnet.transaction`);
      }
    } catch (error) {
      this.logger.error(`解析消息失敗: ${error.message}`);
    }
  }
}
