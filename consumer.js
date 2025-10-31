import 'dotenv/config'; 
import { Kafka, logLevel } from 'kafkajs';
import connectDB from './config/db.js';
import Order from './models/Order.js';
import { getProducer } from './lib/kafka.js';


const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const ORDER_TOPIC = process.env.KAFKA_ORDER_TOPIC || 'orders';
const DLQ_TOPIC = process.env.KAFKA_ORDER_DLQ_TOPIC || 'orders_dlq';
const FAILURE_TOPIC = process.env.KAFKA_ORDER_FAILURE_TOPIC || 'order_failures';
const CONSUMER_GROUP_ID = process.env.KAFKA_CONSUMER_GROUP_ID || 'order-processing-group';


const kafka = new Kafka({
  clientId: 'order-processor',
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.INFO,

});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP_ID });

const runConsumer = async () => {
  try {
    await connectDB();
    console.log('MongoDB Connected for Consumer.');
  } catch (dbError) {
    console.error('Consumer failed to connect to MongoDB:', dbError);
    process.exit(1);
  }

  try {
    await consumer.connect();
    console.log('Kafka Consumer Connected.');
    await consumer.subscribe({ topic: ORDER_TOPIC, fromBeginning: false });
    console.log(`Subscribed to "${ORDER_TOPIC}" topic.`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        let orderDataPayload;
        let rawMessage;
        let order; 

        try {
          rawMessage = JSON.parse(message.value.toString());


          if (rawMessage.eventType !== 'OrderProcessingRequested' || !rawMessage.payload) {
            console.warn('Received message is not a valid OrderProcessingRequested event:', rawMessage);
            return; 
          }
          orderDataPayload = rawMessage.payload;
          const { orderId, userId, items } = orderDataPayload;
          const eventId = rawMessage.eventId;


          order = await Order.findById(orderId);
          if (!order) {
            console.warn(`Order ${orderId} not found. Message might be invalid or DB is lagging.`);
            throw new Error(`Order ${orderId} not found.`);
          }

          if (order.status !== 'Pending') {
            console.log(`Order ${orderId} already processed (Status: ${order.status}). Skipping message.`);
            return; 
          }

          order.status = 'Processing';
          order.eventId = eventId; 
          await order.save();
          console.log(`Processing Order ${order._id} for user ${userId}.`);

          console.log(`(Skipping inventory/notifications) Marking Order ${order._id} as Confirmed.`);
        order.status = 'Confirmed'; 
        await order.save(); 
        
        console.log(`Finished processing Order ${order._id}. Status: ${order.status}`);

        } catch (error) { 
          console.error(`FATAL: Error processing message, sending to DLQ. OrderID: ${order?._id}, EventID: ${rawMessage?.eventId}`, error);
          
          try {
            const dlqProducer = await getProducer();
            const errorMessage = error instanceof Error ? error.message : String(error);
            const errorStack = error instanceof Error ? error.stack : 'N/A';

            await dlqProducer.send({
              topic: DLQ_TOPIC,
              messages: [{
                key: message.key,
                value: message.value,
                headers: {
                  processingError: errorMessage,
                  errorStack: errorStack,
                  originalTopic: topic,
                  originalPartition: String(partition),
                  originalOffset: message.offset,
                  consumerGroupId: CONSUMER_GROUP_ID,
                  timestamp: String(Date.now())
                }
              }]
            });
            console.log(`Message sent to DLQ topic: ${DLQ_TOPIC}`);
          } catch (dlqError) {
            console.error(`CRITICAL: Failed to send message to DLQ:`, dlqError);
          }
        } 
      }, 
    }); 

  } catch (consumerError) {
    console.error('Kafka Consumer fatal error:', consumerError);
    process.exit(1);
  }
};


const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
  process.on(type, async (err) => {
    try {
      console.error(`Unhandled error: ${err}`, err.stack);
      await consumer.disconnect();
      console.log('Kafka Consumer disconnected due to error.');
      process.exit(1);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      console.log(`Received ${type}, shutting down consumer...`);
      await consumer.disconnect();
      console.log('Kafka Consumer disconnected gracefully.');
    } finally {
      process.kill(process.pid, type);
    }
  });
});

runConsumer();