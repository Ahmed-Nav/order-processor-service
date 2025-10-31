# Order Processor Service

![Node.js](https://img.shields.io/badge/Node.js-20+-green?style=for-the-badge&logo=nodedotjs)
![Kafka](https://img.shields.io/badge/Apache_Kafka-black?style=for-the-badge&logo=apachekafka)
![MongoDB](https://img.shields.io/badge/MongoDB-white?style=for-the-badge&logo=mongodb)

## Overview

This is a dedicated, event-driven microservice designed to asynchronously process orders for the **Side Quest E-Commerce Platform**.

This service acts as a Kafka consumer. It subscribes to an `orders` topic, listens for new order messages, and processes them by updating their status in a MongoDB database. This pattern decouples the order creation (in the main e-commerce API) from the order fulfillment/processing logic, ensuring high availability and resilience.

## Core Functionality

* **Kafka Consumer:** Connects to a Kafka cluster and subscribes to the `KAFKA_ORDER_TOPIC` (defaulting to `orders`).
* **Asynchronous Processing:** Listens for incoming order messages and processes them one by one.
* **Database Integration:** Connects to its own MongoDB database using Mongoose.
* **Order Status Updates:** On receiving a message, it finds the corresponding order by its ID and simulates processing by:
    1.  Updating the order status to `Processing`.
    2.  Waiting for 5 seconds.
    3.  Updating the order status to `Completed`.

## Tech Stack

* [**Node.js**](https://nodejs.org/): The runtime environment.
* [**kafkajs**](https://kafka.js.org/): A modern Node.js client for Apache Kafka.
* [**Mongoose**](https://mongoosejs.com/): An Object Data Modeling (ODM) library for MongoDB.
* [**dotenv**](https://github.com/motdotla/dotenv): For managing environment variables.

## Project Structure

```
order-processor-service/
├── config/
│   └── db.js               # MongoDB connection logic
├── lib/
│   └── kafka.js            # Kafka consumer client setup
├── models/
│   └── Order.js            # Mongoose Schema for Orders
├── consumer.js             # Main application logic
├── .env.example            # Environment variable template
├── package.json
└── docker-compose.yaml     # For running Kafka/Zookeeper
```

## Getting Started

### Prerequisites

* Node.js (v18+)
* MongoDB (a local instance or a cloud URI, e.g., from MongoDB Atlas)
* [Docker](https://www.docker.com/products/docker-desktop/) (to run Kafka locally)

### 1. Clone the Repository

```bash
git clone [https://github.com/your-username/order-processor-service.git](https://github.com/your-username/order-processor-service.git)
cd order-processor-service
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Set Up Environment Variables

Copy the example environment file to a new `.env` file.

```bash
cp .env.example .env
```
Now, open the `.env` file and add your secret keys for MongoDB and Kafka.

### 4. Run Local Services (Kafka)

This service requires a running Kafka instance. You can use the `docker-compose.yaml` included in the project root (or from the main `sidequest` e-commerce project).

```bash
docker-compose up -d
```
This will start the required Kafka and Zookeeper services in the background.

### 5. Run the Service

Once Kafka and MongoDB are running, start the consumer service:

```bash
node consumer.js
```

The service will connect to Kafka and MongoDB and begin listening for new messages on the `orders` topic.