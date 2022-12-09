import { Consumer, EachMessagePayload, IHeaders, Kafka } from "kafkajs";
import { program } from "commander";
import { exit } from "process";

const DEFAULT_GROUP_ID = "local";

const DEV_BROKERS = [
  "b-1.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-2.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
  "b-3.smtip-kafka-cluster-w.h52s5m.c14.kafka.us-east-1.amazonaws.com:9094",
];

let firstMessage = true;

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

type Options = {
  outputFile?: string;
  groupId: string;
  topic: string;
  fromBeginning: boolean;
};

class App {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  constructor(private readonly options: Options) {
    this.options = options;
    this.kafka = new Kafka({
      ssl: true,
      brokers: DEV_BROKERS,
      logLevel: 2,
    });

    this.consumer = this.kafka.consumer({
      groupId: this.options.groupId,
    });
  }

  async main(): Promise<void> {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: this.options.topic,
      fromBeginning: this.options.fromBeginning ?? false,
    });
    console.log("subscribed consumer");

    await this.consumer.run({
      autoCommit: false,
      eachMessage: this.onMessage,
    });
  }

  async shutdown(): Promise<void> {
    await this.consumer.disconnect();
    console.log("\n]\n");
  }

  async onMessage({ message }: EachMessagePayload) {
    const messageString = message.value?.toString();
    if (!messageString) {
      return;
    }

    const output = {
      headers: getHeaders(message.headers),
      value: JSON.parse(messageString),
    };

    if (firstMessage) {
      firstMessage = false;
      console.log("[\n");
    } else {
      console.log(",\n");
    }
    console.log(JSON.stringify(output, null, 2));
  }
}

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

const app = new App(options as Options);

process.on("SIGINT", async function () {
  await app.shutdown();
  process.exit();
});

app.main();
