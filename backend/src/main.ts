import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // 啟動 Kafka 微服務監聽
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
      },
      consumer: {
        groupId: 'solana-indexer-consumer',
      },
    },
  });

  await app.startAllMicroservices(); // 啟動 Kafka 消費者
  await app.listen(process.env.PORT ?? 3000); // 啟動 API 服務
  
  console.log(`Indexer is running and listening to Kafka...`);
}
bootstrap();