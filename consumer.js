import "dotenv/config";
import { Kafka, logLevel } from "kafkajs";
import connectDB from "./config/db.js";
import Order from "./models/Order.js";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const kafka = new Kafka({
  clientId: "order-processor",
  brokers: brokers,
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: "order-processing-group" });

const runConsumer = async () => {
  try {
    await connectDB();
    console.log("MongoDB Connected for Consumer.");
  } catch (dbError) {
    console.error("Consumer failed to connect to MongoDB:", dbError);
    process.exit(1);
  }

  try {
    await consumer.connect();
    console.log("Kafka Consumer Connected.");
    await consumer.subscribe({ topic: "orders", fromBeginning: false });
    console.log('Subscribed to "orders" topic.');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Received message from partition ${partition}: ${message.value.toString()}`
        );
        let orderDataPayload;

        try {
          const rawMessage = JSON.parse(message.value.toString());

          if (rawMessage.eventType !== "OrderCreated" || !rawMessage.payload) {
            console.warn(
              "Received message is not a valid OrderCreated event:",
              rawMessage
            );

            return;
          }
          orderDataPayload = rawMessage.payload;

          console.log(
            `Processing order for user: ${orderDataPayload.userId}, Event ID: ${rawMessage.eventId}`
          );

          const newOrder = new Order({
            userId: orderDataPayload.userId,

            items: orderDataPayload.items.map((item) => ({
              product: item.productId,
              quantity: item.quantity,
            })),
            amount: orderDataPayload.amount,
            address: orderDataPayload.address,
            status: "Processing",
            date: new Date(orderDataPayload.orderDate),
          });
          await newOrder.save();
          console.log(
            `Order ${newOrder._id} saved successfully for user ${orderDataPayload.userId}.`
          );

          console.log(`TODO: Update inventory for order ${newOrder._id}`);

          console.log(
            `TODO: Send order confirmation email for order ${newOrder._id}`
          );

          newOrder.status = "Confirmed";
          await newOrder.save();
          console.log(`Order ${newOrder._id} status updated to Confirmed.`);
        } catch (error) {
          console.error(
            `Error processing message for Event ID ${
              orderDataPayload?.eventId || "unknown"
            }:`,
            error
          );
        }
      },
    });
  } catch (consumerError) {
    console.error("Kafka Consumer error:", consumerError);
    process.exit(1);
  }
};

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async (err) => {
    try {
      console.error(`Unhandled error: ${err}`, err.stack);
      await consumer.disconnect();
      console.log("Kafka Consumer disconnected due to error.");
      process.exit(1);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      console.log(`Received ${type}, shutting down consumer...`);
      await consumer.disconnect();
      console.log("Kafka Consumer disconnected gracefully.");
    } finally {
      process.kill(process.pid, type);
    }
  });
});

runConsumer();
