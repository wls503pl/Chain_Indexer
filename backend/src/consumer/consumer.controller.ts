import { EventPattern } from '@nestjs/microservices';
import { Controller, Body } from '@nestjs/common';
import { ConsumerService } from './consumer.service';
// 原项目从 @prisma/client 导入 Slot 类型，现已迁移至本地 domain/types.ts 自定义，移除 Prisma 依赖
import { Account, BlockMeta, Transaction, Slot } from './domain/types';

/**
 * ConsumerController —— Kafka 消息消费者的入口控制器
 *
 * 在 NestJS 微服务模式下，Controller 不处理 HTTP 请求，
 * 而是通过 @EventPattern 监听 Kafka Topic，
 * 收到消息后直接转发给 ConsumerService 做业务处理和数据库写入。
 *
 * 这里监听的五个 Topic：
 *   - solana.testnet.slot_status       : slot 状态变更
 *   - solana.testnet.block_metadata    : 区块元信息
 *   - solana.testnet.account_updates   : 账户状态变更
 *   - solana.testnet.transaction       : Solana 交易数据
 *   - eth.mainnet.uniswap.swap         : Ethereum Uniswap V3 Swap 事件
 */
@Controller({ path: 'consumer', version: '1' })
export class ConsumerController {
  // 通过构造函数注入 ConsumerService，由 NestJS DI 容器管理实例
  constructor(private readonly consumerService: ConsumerService) {}

  /**
   * 监听 Kafka Topic: solana.testnet.slot_status
   * 收到 slot 状态更新消息后，将其中的 Slot 对象持久化到数据库。
   */
  @EventPattern('solana.testnet.slot_status')
  slotStatus(@Body() message: { update_oneof: { Slot: Slot } }) {
    return this.consumerService.saveSlot(message.update_oneof.Slot);
  }

  /**
   * 监听 Kafka Topic: solana.testnet.block_metadata
   * 收到区块元信息消息后，将 BlockMeta 持久化到数据库。
   */
  @EventPattern('solana.testnet.block_metadata')
  blockMetadata(@Body() message: { update_oneof: { BlockMeta: BlockMeta } }) {
    return this.consumerService.saveBlock(message.update_oneof.BlockMeta);
  }

  /**
   * 监听 Kafka Topic: solana.testnet.account_updates
   * 收到账户状态变更消息后，将账户数据和所在 slot 持久化到数据库。
   */
  @EventPattern('solana.testnet.account_updates')
  accountUpdates(
    @Body()
    message: {
      update_oneof: { Account: { account: Account; slot: number } };
    },
  ) {
    return this.consumerService.saveAccount(
      message.update_oneof.Account.account,
      message.update_oneof.Account.slot,
    );
  }

  /**
   * 监听 Kafka Topic: solana.testnet.transaction
   * 收到交易消息后，将交易数据和所在 slot 持久化到数据库。
   */
  @EventPattern('solana.testnet.transaction')
  transaction(
    @Body()
    message: {
      update_oneof: { Transaction: { slot: bigint; transaction: Transaction } };
    },
  ) {
    return this.consumerService.saveTransaction(
      message.update_oneof.Transaction.transaction,
      message.update_oneof.Transaction.slot,
    );
  }

  /**
   * 监听 Kafka Topic: eth.mainnet.uniswap.swap
   * 收到 Ethereum Uniswap V3 Swap 事件后，将其持久化到数据库。
   *
   * 数据由 EthListenerService 通过 Alchemy WebSocket 捕获后发送到 Kafka，
   * 这里消费并转发给 ConsumerService.saveEthSwap() 写入 PostgreSQL。
   *
   * message 结构示例：
   * {
   *   chain: 'ethereum',
   *   network: 'mainnet',
   *   protocol: 'uniswap_v3',
   *   event_type: 'swap',
   *   transaction_hash: '0x...',
   *   block_number: 12345678,
   *   pool_address: '0x...',
   *   swap_data: { sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick },
   *   log_index: 0,
   *   timestamp: 1234567890
   * }
   */
  @EventPattern('eth.mainnet.uniswap.swap')
  ethUniswapSwap(@Body() message: any) {
    return this.consumerService.saveEthSwap(message);
  }
}