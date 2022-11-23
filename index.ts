import { IHeaders, Kafka } from "kafkajs";

const TOPIC = "CI_Campaign_Moderation_Dev";
const FROM_BEGINNING = true;

const DEV = [
  "b-1.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-2.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-3.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
];

const getHeaders = (
  messageHeaders?: IHeaders
): Record<string, string | string[]> => {
  const headers: Record<string, string | string[]> = {};
  if (messageHeaders) {
    for (const [k, val] of Object.entries(messageHeaders)) {
      if (val) {
        if (Array.isArray(val)) {
          headers[k] = val.map((item) => item.toString());
        } else {
          headers[k] = val.toString();
        }
      }
    }
  }
  return headers;
};

async function run() {
  const kafka = new Kafka({
    ssl: true,
    brokers: DEV,
    logLevel: 2,
  });

  const consumer = kafka.consumer({ groupId: "ian-gilham-local" });
  await consumer.connect();

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: FROM_BEGINNING,
  });

  consumer.run({
    eachMessage: async ({ message }) => {
      const messageString = message.value?.toString();
      if (!messageString) {
        console.log('empty message');
      }
      const headers = getHeaders(message.headers);

      console.log(JSON.stringify({
        headers,
        value: JSON.parse(messageString)
      }, null, 2));
    },
  });
}

run();
