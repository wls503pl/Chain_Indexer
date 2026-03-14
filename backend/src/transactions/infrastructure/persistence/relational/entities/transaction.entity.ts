import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { EntityRelationalHelper } from '../../../../../utils/relational-entity-helper';
import { ApiProperty } from '@nestjs/swagger';

/**
 * TransactionEntity —— 数据库 `transaction` 表的 TypeORM 实体映射
 *
 * 对应链上每笔交易的完整数据，由 ConsumerService.saveTransaction() 写入。
 * 数据来源：Kafka Topic `solana.testnet.transaction`
 *
 * 与 BlockEntity 的设计差异：
 *   - 主键使用自增 UUID（而非业务主键 signature），
 *     原因是同一笔交易理论上只写入一次，UUID 更通用且便于关联查询。
 *   - transaction / meta 两个核心字段使用 jsonb 存储，
 *     保留原始链上数据结构，便于后续按需解析，不在数据库层做拆解。
 *   - 自动维护 createdAt / updatedAt 时间戳，方便排查数据写入时序问题。
 */
@Entity({
  name: 'transaction', // 映射到数据库中名为 "transaction" 的表
})
export class TransactionEntity extends EntityRelationalHelper {
  // ── 主键 ──────────────────────────────────────────────────────────────────

  @ApiProperty()
  @PrimaryGeneratedColumn('uuid') // 自动生成 UUID 作为主键，无需业务层传入
  id: string;

  // ── 链上定位信息 ───────────────────────────────────────────────────────────

  @ApiProperty()
  @Column({ type: 'bigint' }) // 数据库层用 bigint 存储，ORM 映射为 string 防止 JS 精度丢失
  slot: string; // 该交易所在的 slot 编号（写入时由 Controller 层单独传入）

  @ApiProperty()
  @Column()
  signature: string; // 交易签名（Base58 编码），链上唯一标识符，建议加唯一索引防重复写入

  // ── 交易基础属性 ───────────────────────────────────────────────────────────

  @ApiProperty()
  @Column({ type: 'boolean', default: false })
  is_vote: boolean; // 是否为验证者投票交易（投票交易占 Solana 总交易量约 80%，通常过滤不展示）

  // ── 原始链上数据（jsonb 存储，保留完整结构）──────────────────────────────────

  @ApiProperty()
  @Column({ type: 'jsonb', nullable: true })
  transaction: any; // 完整交易体（含签名列表、指令、账户等），对应 Transaction.transaction 字段

  @ApiProperty()
  @Column({ type: 'jsonb', nullable: true })
  meta: any; // 交易执行结果元数据（费用、余额变化、日志、CPI 指令等），对应 Transaction.meta 字段

  // ── 辅助字段 ───────────────────────────────────────────────────────────────

  @ApiProperty()
  @Column({ nullable: true })
  index: number; // 该交易在所在 slot 中的排列索引，用于还原区块内交易顺序

  @ApiProperty()
  @CreateDateColumn() // TypeORM 自动在 INSERT 时填充当前时间
  createdAt: Date; // 记录写入数据库的时间（非链上时间）

  @ApiProperty()
  @UpdateDateColumn() // TypeORM 自动在 UPDATE 时更新为当前时间
  updatedAt: Date; // 记录最后一次更新时间
}
