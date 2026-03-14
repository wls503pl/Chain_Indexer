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
 * 这里监听的四个 Topic 对应 Yellowstone gRPC 推送的四类链上事件：
 *   - slot_status       : slot 状态变更（已处理 / 已确认 / 已最终确认）
 *   - block_metadata    : 区块元信息
 *   - account_updates   : 账户状态变更
 *   - transaction       : 交易数据
 */
@Controller({ path: 'consumer', version: '1' })
export class ConsumerController {
  // 通过构造函数注入 ConsumerService，由 NestJS DI 容器管理实例
  constructor(private readonly consumerService: ConsumerService) {}

  /**
   * 监听 Kafka Topic: solana.testnet.slot_status
   * 收到 slot 状态更新消息后，将其中的 Slot 对象持久化到数据库。
   *
   * Solana 的 slot 有三个确认阶段：
   *   processed（已处理）→ confirmed（已确认）→ finalized（已最终确认）
   * 索引器通常只存 finalized 状态，避免因 reorg 导致数据回滚。
   *
   * message 结构示例：
   * { update_oneof: { Slot: { slot: 123n, parent: 122n, status: 'finalized' } } }
   */
  @EventPattern('solana.testnet.slot_status')
  slotStatus(@Body() message: { update_oneof: { Slot: Slot } }) {
    return this.consumerService.saveSlot(message.update_oneof.Slot);
  }

  /**
   * 监听 Kafka Topic: solana.testnet.block_metadata
   * 收到区块元信息消息后，将 BlockMeta 持久化到数据库。
   *
   * BlockMeta 包含区块哈希、时间戳、父块信息、交易数量等，
   * 是后续查询区块详情的基础索引数据。
   *
   * message 结构示例：
   * { update_oneof: { BlockMeta: { slot: 123n, blockhash: '...', ... } } }
   */
  @EventPattern('solana.testnet.block_metadata')
  blockMetadata(@Body() message: { update_oneof: { BlockMeta: BlockMeta } }) {
    return this.consumerService.saveBlock(message.update_oneof.BlockMeta);
  }

  /**
   * 监听 Kafka Topic: solana.testnet.account_updates
   * 收到账户状态变更消息后，将账户数据和所在 slot 持久化到数据库。
   *
   * 注意 message 的嵌套结构：Account 字段下同时携带了
   *   - account : 账户完整状态（公钥、余额、数据等）
   *   - slot    : 触发此次变更的 slot 编号
   * Service 层需要两个参数分开处理，所以这里解构后分别传入。
   *
   * message 结构示例：
   * { update_oneof: { Account: { account: { pubkey: '...', lamports: 100 }, slot: 123 } } }
   */
  @EventPattern('solana.testnet.account_updates')
  accountUpdates(
    @Body()
    message: {
      update_oneof: { Account: { account: Account; slot: number } };
    },
  ) {
    return this.consumerService.saveAccount(
      message.update_oneof.Account.account, // 账户状态对象
      message.update_oneof.Account.slot, // 触发变更的 slot
    );
  }

  /**
   * 监听 Kafka Topic: solana.testnet.transaction
   * 收到交易消息后，将交易数据和所在 slot 持久化到数据库。
   *
   * 同 accountUpdates，Transaction 字段下也同时携带了
   *   - transaction : 完整交易对象（签名、指令、执行结果 meta 等）
   *   - slot        : 该交易所在的 slot（bigint，Solana slot 编号较大需用 bigint）
   *
   * message 结构示例：
   * { update_oneof: { Transaction: { slot: 123n, transaction: { signature: '...', ... } } } }
   */
  @EventPattern('solana.testnet.transaction')
  transaction(
    @Body()
    message: {
      update_oneof: { Transaction: { slot: bigint; transaction: Transaction } };
    },
  ) {
    return this.consumerService.saveTransaction(
      message.update_oneof.Transaction.transaction, // 完整交易对象
      message.update_oneof.Transaction.slot, // 交易所在 slot
    );
  }
}
