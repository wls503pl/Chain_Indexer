import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { SolanaListenerService } from './solana-listener.service';
import { EthListenerService } from './eth-listener.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'GEYSER_SERVICE', // 复用你 consumer.module.ts 里的注入名
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'chain-indexer-producer',
            brokers: ['localhost:9092'],
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  providers: [SolanaListenerService, EthListenerService],
  exports: [SolanaListenerService, EthListenerService],
})
export class ListenerModule {}