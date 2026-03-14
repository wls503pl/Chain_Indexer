import { Entity, Column, PrimaryColumn } from 'typeorm';
import { EntityRelationalHelper } from '../../../../../utils/relational-entity-helper';
import { ApiProperty } from '@nestjs/swagger';

/**
 * BlockEntity —— 数据库 `block` 表的 TypeORM 实体映射
 *
 * 对应链上每个区块的元信息，由 ConsumerService.saveBlock() 写入。
 * 数据来源：Kafka Topic `solana.testnet.block_metadata`
 *
 * 注意：所有数值型字段（bigint / number）在数据库层均存为字符串或 bigint 类型，
 * 原因是 JavaScript 的 number 最大安全整数为 2^53-1，
 * 而 Solana 的 slot / block_height 已超过这个范围，字符串存储可避免精度丢失。
 */
@Entity({ name: 'block' }) // 映射到数据库中名为 "block" 的表
export class BlockEntity extends EntityRelationalHelper {
  // ── 主键 ──────────────────────────────────────────────────────────────────

  @ApiProperty()
  @PrimaryColumn({ type: 'bigint' }) // 数据库层用 bigint 存储，ORM 映射为 string 防止 JS 精度丢失
  slot: bigint; // Solana slot 编号，全局唯一，作为区块的主键

  // ── 区块基础信息 ───────────────────────────────────────────────────────────

  @ApiProperty()
  @Column()
  blockhash: string; // 区块哈希，用于唯一标识一个区块内容（非 null）

  @ApiProperty()
  @Column({ nullable: true })
  rewards: string; // 区块奖励列表，由 JSON.stringify 序列化后存入（原始类型为 any[]）

  @ApiProperty()
  @Column({ nullable: true })
  block_time: string; // 区块生成时间，存为 Unix 时间戳字符串（原始类型为 number）

  @ApiProperty()
  @Column({ nullable: true })
  block_height: string; // 区块高度，存为字符串（原始类型为 bigint，防精度丢失）

  // ── 父区块信息（用于链式结构校验 / reorg 检测）────────────────────────────

  @ApiProperty()
  @Column({ type: 'bigint', nullable: true })
  parent_slot: number; // 父区块的 slot 编号，数据库存 bigint，ORM 映射为 number

  @ApiProperty()
  @Column({ nullable: true })
  parent_blockhash: string; // 父区块的哈希值

  // ── 统计信息 ───────────────────────────────────────────────────────────────

  @ApiProperty()
  @Column({ nullable: true })
  executed_transaction_count: string; // 本 slot 实际执行的交易数量，存为字符串

  @ApiProperty()
  @Column({ nullable: true })
  entries_count: string; // 本 slot 的 entry 数量（PoH 基本单元），存为字符串
}
