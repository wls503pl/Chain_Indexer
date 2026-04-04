import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { BlockEntity } from '../blocks/infrastructure/persistence/relational/entities/block.entity';
import { TransactionEntity } from '../transactions/infrastructure/persistence/relational/entities/transaction.entity';
import { Account, BlockMeta, Transaction, Slot } from './domain/types';

/**
 * ConsumerService —— Kafka 消息的业务处理层
 *
 * 职责：接收 Controller 解析好的链上数据对象，
 * 做必要的类型转换后，通过 TypeORM Repository 写入数据库。
 */
@Injectable()
export class ConsumerService {
  private readonly logger = new Logger(ConsumerService.name);

  constructor(
    @InjectRepository(BlockEntity)
    private readonly blockRepository: Repository<BlockEntity>,

    @InjectRepository(TransactionEntity)
    private readonly transactionRepository: Repository<TransactionEntity>,
  ) {}

  /**
   * 保存 Slot 状态（当前仅打印日志）
   */
  saveSlot(data: Slot) {
    this.logger.log(
      `Received slot status: slot=${data.slot}, status=${data.status}`,
    );
  }

  /**
   * 保存区块元信息（以 slot 为唯一键做 upsert）
   */
  saveBlock(data: BlockMeta) {
    return this.blockRepository
      .createQueryBuilder()
      .insert()
      .into(BlockEntity)
      .values({
        slot: data.slot,
        blockhash: data.blockhash,
        block_time: data.block_time?.timestamp?.toString() ?? null,
        block_height: data.block_height?.block_height?.toString() ?? null,
        parent_slot: Number(data.parent_slot),
        parent_blockhash: data.parent_blockhash,
        rewards: JSON.stringify(data.rewards?.rewards) ?? null,
        executed_transaction_count:
          data.executed_transaction_count?.toString() ?? null,
        entries_count: data.entries_count?.toString() ?? null,
      })
      .orUpdate(
        [
          'blockhash',
          'block_time',
          'block_height',
          'parent_slot',
          'parent_blockhash',
          'rewards',
          'executed_transaction_count',
          'entries_count',
        ],
        ['slot'],
      )
      .execute();
  }

  /**
   * 保存账户状态（当前仅打印日志）
   */
  saveAccount(account: Account, slot: number) {
    this.logger.log(
      `Received account update: pubkey=${account.pubkey}, slot=${slot}`,
    );
  }

  /**
   * 保存 Solana 交易数据
   */
  saveTransaction(transaction: Transaction, slot: bigint) {
    const tx = this.transactionRepository.create({
      slot: slot.toString(),
      signature: transaction.signature,
      is_vote: transaction.is_vote,
      transaction: transaction.transaction,
      meta: transaction.meta,
    });
    return this.transactionRepository.save(tx);
  }

  /**
   * 保存 Ethereum Uniswap V3 Swap 事件
   *
   * 将 Ethereum 事件数据映射到现有的 TransactionEntity 结构：
   *   - signature   → 使用 transaction_hash + log_index 组合作为唯一标识
   *                    （同一笔 ETH 交易可能产生多个 Swap 事件，所以需要 log_index 区分）
   *   - slot        → 使用 block_number（Ethereum 的"slot"等价物）
   *   - is_vote     → 固定为 false（Ethereum 没有投票交易的概念）
   *   - transaction → 存储完整的 swap_data 和元信息（jsonb）
   *   - meta        → 存储链/协议/事件类型等元数据（jsonb）
   *
   * 幂等性：通过 signature 的唯一索引保证，重复插入会被数据库拒绝。
   */
  async saveEthSwap(data: any) {
    try {
      // 组合 transaction_hash + log_index 作为唯一标识
      // 同一笔 ETH 交易可能触发多个 Swap 事件（如多跳交易）
      const uniqueSignature = `${data.transaction_hash}-${data.log_index ?? 0}`;

      const tx = this.transactionRepository.create({
        slot: (data.block_number ?? 0).toString(),
        signature: uniqueSignature,
        chain: 'ethereum',
        is_vote: false,
        transaction: {
          chain: data.chain,
          network: data.network,
          protocol: data.protocol,
          event_type: data.event_type,
          transaction_hash: data.transaction_hash,
          block_number: data.block_number,
          pool_address: data.pool_address,
          swap_data: data.swap_data,
          log_index: data.log_index,
          timestamp: data.timestamp,
        },
        meta: {
          chain: data.chain,
          network: data.network,
          protocol: data.protocol,
          event_type: data.event_type,
        },
      });

      await this.transactionRepository.save(tx);
      this.logger.log(
        `ETH Swap 已写入数据库: ${data.transaction_hash} (Block: ${data.block_number})`,
      );
    } catch (error) {
      // 如果是唯一索引冲突（重复消息），记录警告而非报错
      if (error.code === '23505') {
        this.logger.warn(
          `ETH Swap 已存在，跳过: ${data.transaction_hash}`,
        );
      } else {
        this.logger.error(
          `ETH Swap 写入失败: ${error.message}`,
          error.stack,
        );
      }
    }
  }
}