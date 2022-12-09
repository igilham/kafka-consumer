import { IHeaders, Kafka } from "kafkajs";
import { program } from "commander";
import { exit } from "process";

const DEFAULT_GROUP_ID = "local";

const DEV_BROKERS = [
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

async function main() {
  program
    .name("kafka consumer")
    .description("Subscribes to a kafka topic and prints out messages")
    .version("0.0.1")
    .option(
      "-b, --from-beginning",
      "start reading from the oldest available message",
      false
    )
    .option("-t, --topic <string>", "topic to read from")
    .option(
      "-g, --group-id <string>",
      "group ID of the subscriber",
      DEFAULT_GROUP_ID
    );
  program.parse();
  const options = program.opts();

  if (!options.topic) {
    console.error("topic not specified");
    exit(1);
  }

  const kafka = new Kafka({
    ssl: true,
    brokers: DEV_BROKERS,
    logLevel: 2,
  });

  const consumer = kafka.consumer({
    groupId: options.groupId,
  });
  await consumer.connect();

  await consumer.subscribe({
    topic: options.topic,
    fromBeginning: options.fromBeginning ?? false,
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ message }) => {
      const messageString = message.value?.toString() ?? "";
      if (messageString.length === 0) {
        console.log("empty message");
        return;
      }
      const headers = getHeaders(message.headers);

      console.log(
        JSON.stringify(
          {
            headers,
            value: JSON.parse(messageString),
          },
          null,
          2
        )
      );
    },
  });
}

main();
