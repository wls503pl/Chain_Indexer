import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
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
  @Index({ unique: true })
  @Column()
  signature: string;

  @ApiProperty()
  @Column({ type: 'varchar', length: 20, default: 'solana' })
  chain: string; // 'solana' | 'ethereum' — 標識交易來源鏈

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