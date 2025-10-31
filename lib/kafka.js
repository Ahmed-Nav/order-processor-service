//order-processor-service/lib/kafka.js
import { Kafka, logLevel } from 'kafkajs';
import 'dotenv/config';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

const kafka = new Kafka({
  clientId: 'order-processor-producer', 
  brokers: brokers,
  logLevel: logLevel.WARN, 
});

let producerInstance = null;
let producerConnecting = false;


export const getProducer = async () => {
  if (producerInstance) {
    return producerInstance;
  }
  if (producerConnecting) {
    await new Promise(resolve => setTimeout(resolve, 100));
    return getProducer();
  }

  producerConnecting = true;
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
  });

  try {
    console.log('Connecting Kafka DLQ/Internal Producer...');
    await producer.connect();
    console.log('Kafka DLQ/Internal Producer Connected.');
    producerInstance = producer;

    producer.on('producer.disconnect', (event) => {
      console.error('Kafka DLQ/Internal Producer disconnected!', event);
      producerInstance = null;
      producerConnecting = false;
    });

  } catch (error) {
    console.error('Failed to connect Kafka DLQ/Internal producer:', error);
    producerConnecting = false;
    throw error;
  } finally {
    producerConnecting = false;
  }

  return producerInstance;
};