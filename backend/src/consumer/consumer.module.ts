import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
// 开始使用 PrismaModule 提供数据库访问, 结果老是报错
// 现已迁移至 TypeORM,改用 TypeOrmModule.forFeature 注册实体
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConsumerController } from './consumer.controller';
import { ConsumerService } from './consumer.service';
// BlockEntity 映射 block 表,用于保存区块元信息
import { BlockEntity } from '../blocks/infrastructure/persistence/relational/entities/block.entity';
// TransactionEntity 映射 transaction 表,用于保存交易数据
import { TransactionEntity } from '../transactions/infrastructure/persistence/relational/entities/transaction.entity';

/**
 * ConsumerModule —— Kafka 消费者功能模块
 *
 * NestJS 的模块是功能边界的基本单元,这个模块负责:
 *   1. 注册 Kafka 客户端连接配置(ClientsModule)
 *   2. 声明消费者控制器和服务
 *   3. 导入数据库实体(TypeOrmModule.forFeature)供 Service 通过 Repository 写入数据
 *
 * 整体数据流:
 *   Kafka Broker → ConsumerController(@EventPattern 监听)→ ConsumerService(写库)
 */
@Module({
  imports: [
    /**
     * TypeOrmModule.forFeature —— 在当前模块作用域内注册实体对应的 Repository
     *
     * 注册后,ConsumerService 可通过 @InjectRepository(BlockEntity) 等装饰器
     * 注入对应的 Repository 实例,用于执行数据库读写操作。
     * 这是 TypeORM 替代原项目 PrismaModule 的核心方式。
     */
    TypeOrmModule.forFeature([BlockEntity, TransactionEntity]),

    /**
     * ClientsModule.register —— 向 NestJS DI 容器注册 Kafka 客户端
     *
     * 注册后可在任意 Service/Controller 中通过
     * @Inject('GEYSER_SERVICE') 注入该客户端,
     * 用于主动向 Kafka 发送消息(生产者角色)。
     *
     * 注意:这里的配置同时也决定了消费者连接哪个 Broker。
     */
    ClientsModule.register([
      {
        name: 'GEYSER_SERVICE', // 注入 Token,其他模块用这个字符串来引用此客户端
        transport: Transport.KAFKA, // 传输层协议指定为 Kafka(NestJS 还支持 Redis、gRPC 等)
        options: {
          client: {
            brokers: ['localhost:9092'], // Kafka Broker 地址,生产环境应从环境变量读取
          },
          consumer: {
            groupId: 'solana-indexer', // 消费者组 ID,同组内多个实例会自动负载均衡分配 partition
          },
        },
      },
    ]),
  ],

  // 声明本模块的控制器,NestJS 会自动扫描其中的 @EventPattern 并注册为 Kafka 消息监听器
  controllers: [ConsumerController],

  // 声明本模块的服务,由 NestJS DI 容器管理生命周期
  providers: [ConsumerService],

  // 将 ConsumerService 导出,允许其他模块(如 AppModule)注入并调用其方法
  exports: [ConsumerService],
})
export class ConsumerModule {}
