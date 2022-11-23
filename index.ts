import { Kafka } from "kafkajs";

const TOPIC = "CI_Campaign_Moderation_Dev";
const FROM_BEGINNING = true;

const DEV = [
  "b-1.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-2.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-3.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
];

async function run() {
  const kafka = new Kafka({
    ssl: true,
    brokers: DEV,
    logLevel: 2,
  });

  const consumer = kafka.consumer({ groupId: "louis-local" });
  await consumer.connect();

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: FROM_BEGINNING,
  });

  consumer.run({
    eachMessage: async ({ message }) => {
      const messageString = message.value?.toString();
      if (!messageString) {
        return;
      }

      console.log(JSON.stringify(JSON.parse(messageString), null, 2));
    },
  });
}

run();
