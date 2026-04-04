import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
  Inject,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { ethers } from 'ethers';

@Injectable()
export class EthListenerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EthListenerService.name);
  private provider: ethers.WebSocketProvider | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private isShuttingDown = false;
  private iface: ethers.Interface;

  private readonly ALCHEMY_WS_URL =
    'wss://eth-mainnet.g.alchemy.com/v2/2Pc6Ms3EX5OoAN9maUcmdhYkME-NAja6';

  private readonly UNISWAP_V3_POOL = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640';

  private readonly SWAP_ABI = [
    'event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)',
  ];

  // Swap 事件的 topic0（keccak256 of event signature）
  private readonly SWAP_TOPIC =
    '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67';

  private readonly KAFKA_TOPIC = 'eth.mainnet.uniswap.swap';

  constructor(
    @Inject('GEYSER_SERVICE') private readonly kafkaClient: ClientKafka,
  ) {
    this.iface = new ethers.Interface(this.SWAP_ABI);
  }

  async onModuleInit() {
    this.logger.log('正在初始化 Ethereum 監聽服務...');
    try {
      await this.kafkaClient.connect();
      this.logger.log('Kafka 生產者連接成功！');
    } catch (err) {
      this.logger.error(`Kafka 生產者連接失敗: ${err.message}`);
    }
    this.connect();
  }

  onModuleDestroy() {
    this.isShuttingDown = true;
    this.logger.log('正在銷毀 Ethereum 監聽服務...');
    this.cleanup();
  }

  private cleanup() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.provider) {
      this.provider.removeAllListeners();
      this.provider.destroy();
      this.provider = null;
    }
  }

  private connect() {
    try {
      this.logger.log('正在連接 Alchemy WebSocket (Ethereum Mainnet)...');

      this.provider = new ethers.WebSocketProvider(this.ALCHEMY_WS_URL);

      // 直接用 provider.on(filter) 監聽原始 log，比 contract.on() 更可靠
      const filter = {
        address: this.UNISWAP_V3_POOL,
        topics: [this.SWAP_TOPIC],
      };

      this.provider.on(filter, (log: ethers.Log) => {
        this.handleRawLog(log);
      });

      const ws = this.provider.websocket as any;

      ws.on('open', () => {
        this.logger.log('Alchemy WebSocket 連接成功！開始監聽 Uniswap V3 Swap 事件...');
        this.logger.log(`監聽合約: ${this.UNISWAP_V3_POOL}`);
      });

      ws.on('close', () => {
        if (!this.isShuttingDown) {
          this.logger.warn('Alchemy WebSocket 連接斷開，10秒後嘗試重連...');
          this.scheduleReconnect();
        }
      });

      ws.on('error', (error: Error) => {
        this.logger.error(`Alchemy WebSocket 報錯: ${error.message}`);
      });
    } catch (error) {
      this.logger.error(`連接失敗: ${error.message}`);
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.isShuttingDown || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.cleanup();
      this.connect();
    }, 10000);
  }

  private handleRawLog(log: ethers.Log) {
    try {
      this.logger.log(
        `捕獲到原始 Log: tx=${log.transactionHash} block=${log.blockNumber}`,
      );

      const parsed = this.iface.parseLog({
        topics: [...log.topics],
        data: log.data,
      });

      if (!parsed) {
        this.logger.warn('parseLog 返回 null，跳過');
        return;
      }

      this.logger.log(
        `捕獲到 Uniswap Swap: ${log.transactionHash} (Block: ${log.blockNumber})`,
      );

      const kafkaPayload = {
        chain: 'ethereum',
        network: 'mainnet',
        protocol: 'uniswap_v3',
        event_type: 'swap',
        transaction_hash: log.transactionHash,
        block_number: log.blockNumber,
        pool_address: this.UNISWAP_V3_POOL,
        swap_data: {
          sender: parsed.args[0],
          recipient: parsed.args[1],
          amount0: parsed.args[2]?.toString() ?? '0',
          amount1: parsed.args[3]?.toString() ?? '0',
          sqrtPriceX96: parsed.args[4]?.toString() ?? '0',
          liquidity: parsed.args[5]?.toString() ?? '0',
          tick: Number(parsed.args[6] ?? 0),
        },
        log_index: log.index,
        timestamp: Date.now(),
      };

      this.kafkaClient.emit(this.KAFKA_TOPIC, {
        key: log.transactionHash,
        value: kafkaPayload,
      });

      this.logger.log(`數據已發送至 Kafka Topic: ${this.KAFKA_TOPIC}`);
    } catch (error) {
      this.logger.error(`處理 Log 失敗: ${error.message}`, error.stack);
    }
  }
}
