import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index, // 1. 導入 Index
} from 'typeorm';
import { EntityRelationalHelper } from '../../../../../utils/relational-entity-helper';
import { ApiProperty } from '@nestjs/swagger';

@Entity({
  name: 'transaction',
})
export class TransactionEntity extends EntityRelationalHelper {
  @ApiProperty()
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @ApiProperty()
  @Column({ type: 'bigint' })
  slot: string;

  @ApiProperty()
  @Index({ unique: true }) // 2. 關鍵：為 signature 增加唯一索引，確保數據庫層面的冪等性
  @Column()
  signature: string;

  @ApiProperty()
  @Column({ type: 'boolean', default: false })
  is_vote: boolean;

  @ApiProperty()
  @Column({ type: 'jsonb', nullable: true })
  transaction: any;

  @ApiProperty()
  @Column({ type: 'jsonb', nullable: true })
  meta: any;

  @ApiProperty()
  @Column({ nullable: true })
  index: number;

  @ApiProperty()
  @CreateDateColumn()
  createdAt: Date;

  @ApiProperty()
  @UpdateDateColumn()
  updatedAt: Date;
}
