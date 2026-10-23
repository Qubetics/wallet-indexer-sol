import { Kafka } from 'kafkajs';
import * as Interface from '../interfaces/kafka.interface';
import { config } from '.';

class KafkaHelper {
    public kafka: any;
    private producer: any;

    constructor() {
        // Do not connect here to avoid async issues
    }

    public async connect() {
        try {
            const KAFKA_BROKER = config.config.KAFKA_BROKER.split(',');
            console.log('KAFKA_BROKER:', KAFKA_BROKER);

            const kafkaCredentials: Interface.KafkaCrednetials = {
                clientId: String(config.config.KAFKA_CLIENT_ID),
                brokers: KAFKA_BROKER,
            };

            this.kafka = new Kafka(kafkaCredentials);
            this.producer = this.kafka.producer();
            await this.producer.connect();
            console.log('✅ Kafka producer connected');
        } catch (error) {
            console.error('❌ Error connecting Kafka producer:', error);
        }
    }

    public async produceMessage(topic: string, messageData: any) {
        try {
            if (!this.producer) {
                throw new Error('Kafka producer is not connected');
            }

            await this.producer.send({
                topic,
                acks: 1,
                messages: [{ value: JSON.stringify(messageData) }],
            });

        } catch (error) {
            console.error('🔥 produceMessage error:', error);
        }
    }

    public async disconnect() {
        try {
            if (this.producer) {
                await this.producer.disconnect();
                console.log('✅ Kafka producer disconnected');
            }
        } catch (error) {
            console.error('❌ Error disconnecting Kafka producer:', error);
        }
    }
}

export default new KafkaHelper();
