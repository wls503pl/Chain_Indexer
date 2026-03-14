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
 *
 * 幂等性：同一条数据写两次结果一致，不会报错也不会产生重复行。
 * TypeORM 使用 save() 方法实现相同效果：
 *   - 对于有主键的实体，save() 会先查询是否存在，存在则 update，不存在则 insert。
 * （saveTransaction 例外，见方法注释）
 */
@Injectable()
export class ConsumerService {
  // NestJS 内置日志工具，日志前缀会自动标注为 "ConsumerService"，便于在日志里定位来源
  private readonly logger = new Logger(ConsumerService.name);

  constructor(
    // 注入 BlockEntity 对应的 Repository，用于区块数据的增删改查
    // @InjectRepository(BlockEntity) → this.blockRepository.save(...)
    @InjectRepository(BlockEntity)
    private readonly blockRepository: Repository<BlockEntity>,

    // 注入 TransactionEntity 对应的 Repository，用于交易数据的增删改查
    @InjectRepository(TransactionEntity)
    private readonly transactionRepository: Repository<TransactionEntity>,
  ) {}

  /**
   * 保存 Slot 状态
   *
   * 当前版本暂时只打印日志，因为我们的 Entity 尚未建立 Slot 专属表。
   * 后续如需存储 slot 状态，可新增 SlotEntity 并在此调用 slotRepository.save()。
   */
  saveSlot(data: Slot) {
    // TODO: 如需持久化 slot 状态，新增 SlotEntity 并替换此处逻辑
    this.logger.log(
      `Received slot status: slot=${data.slot}, status=${data.status}`,
    );
  }

  /**
   * 保存区块元信息
   *
   * 以 slot 编号为唯一键做 upsert（TypeORM 的 save() 在主键存在时执行 update）。
   * 需要对字段做类型转换，对齐 BlockEntity 的字段定义：
   *
   * 1. slot：原始数据是 bigint，BlockEntity.slot 是 number 类型，直接赋值即可
   *    （TypeORM 会在写库时自动处理 bigint → number 的转换）。
   *
   * 2. block_time：原始数据是 { timestamp: number } 嵌套对象（Unix 秒级时间戳），
   *    BlockEntity.block_time 是 string，需解包并转为字符串。
   *
   * 3. block_height：原始数据是 { block_height: bigint } 嵌套对象，
   *    BlockEntity.block_height 是 string，需解包并转为字符串。
   *
   * 4. rewards：原始数据是 any[]，BlockEntity.rewards 是 string，
   *    需 JSON.stringify 序列化后存入。
   *
   * 字段名全部使用下划线命名（snake_case），与 BlockEntity 保持一致。
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
        parent_slot: Number(data.parent_slot), // bigint → number
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
        ['slot'], // 冲突检测字段（主键）
      )
      .execute();
  }

  /**
   * 保存账户状态
   *
   * 原项目以 pubkey（账户公钥）为唯一键做 upsert。
   * 当前版本暂时只打印日志，因为我们的 Entity 尚未建立 Account 专属表。
   * 后续如需存储账户状态，可新增 AccountEntity 并在此调用 accountRepository.save()。
   *
   * 几个字段处理说明（供后续扩展参考）：
   *   - lamport：原始字段名是 lamports（复数），数据库字段是 lamport（单数），注意映射
   *   - rent_epoch：原始是 number，数据库存 String，需 .toString() 转换
   *     （rent_epoch 值可能很大，存字符串避免精度丢失）
   *   - slot：从 Controller 层单独传入，不在 account 对象里
   *   - data / owner / txn_signature：结构复杂，直接透传（数据库对应 JSON 字段）
   */
  saveAccount(account: Account, slot: number) {
    // TODO: 如需持久化账户状态，新增 AccountEntity 并替换此处逻辑
    this.logger.log(
      `Received account update: pubkey=${account.pubkey}, slot=${slot}`,
    );
  }

  /**
   * 保存交易数据
   *
   * 注意：这里用的是 create + save 而非 upsert。
   * 原因：交易签名天然唯一且不可变，同一笔交易不会被更新，
   * 只需插入一次。如果需要防重复，应在数据库层对 signature 加唯一索引，
   * 让重复插入直接报错或用 createOrSkip 处理。
   *
   * transaction 和 meta 字段结构复杂（嵌套对象），
   * 数据库对应 JSON 类型（jsonb），TypeORM 可直接存入，无需额外类型转换。
   *
   * slot 从 Controller 层单独传入（bigint 类型），不在 transaction 对象的顶层。
   *
   * 字段名全部使用下划线命名（snake_case），与 TransactionEntity 保持一致：
   *   - is_vote（不是 isVote）
   *   - transaction / meta（直接存原始对象，不做 JSON.stringify，因为字段类型是 jsonb）
   */
  saveTransaction(transaction: Transaction, slot: bigint) {
    const tx = this.transactionRepository.create({
      slot: slot.toString(), // bigint → string，对应 TransactionEntity.slot 的 bigint 字段
      signature: transaction.signature, // 交易签名，唯一标识符
      is_vote: transaction.is_vote, // 字段名与 TransactionEntity 保持一致，使用 snake_case
      transaction: transaction.transaction, // 完整交易原始数据，存入 jsonb 字段
      meta: transaction.meta, // 交易执行结果 meta，存入 jsonb 字段
    });
    return this.transactionRepository.save(tx);
    // TypeORM save()：直接 INSERT，等效于 Prisma 的 create
  }
}
