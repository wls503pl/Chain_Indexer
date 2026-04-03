import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ListenerModule } from './listener/listener.module';
import { ConsumerModule } from './consumer/consumer.module';
import { TransactionEntity } from './transactions/infrastructure/persistence/relational/entities/transaction.entity';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get<string>('DATABASE_HOST'),
        port: configService.get<number>('DATABASE_PORT'),
        username: configService.get<string>('DATABASE_USER'),
        password: configService.get<string>('DATABASE_PASSWORD'),
        database: configService.get<string>('DATABASE_NAME'),
        entities: [TransactionEntity],
        synchronize: true,
      }),
      inject: [ConfigService],
    }),
    ListenerModule,
    ConsumerModule,
  ],
})
export class AppModule {}
