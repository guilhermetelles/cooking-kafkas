const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "cooking-kafkas",
  brokers: ["localhost:9092"],
});

const topicName = "cooked-kafkas";
const consumerType = process.argv[2];
if (!consumerType) {
  console.error("ERROR: Please input a consumer type.");
  process.exit(1);
}

const processConsumer = async () => {
  switch (consumerType) {
    case "print": {
      const printConsumer = kafka.consumer({ groupId: "print-group" });
      await printConsumer.connect();
      await printConsumer.subscribe({ topic: topicName });

      await printConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const text = `${message.key.toString()}: ${message.value.toString()}`;
          logMessage(topic, partition, text);
        },
      });
      break;
    }
    case "sum": {
      const sumConsumer = kafka.consumer({ groupId: "sum-group" });
      await sumConsumer.connect();
      await sumConsumer.subscribe({ topic: topicName });

      let sum = 0;

      await sumConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const key = message.key.toString();
          if (key === "randomNumber") {
            const value = message.value.toString();
            sum += parseInt(value);
            const text = `current number: ${value} | sum of random numbers: ${sum}`;
            logMessage(topic, partition, text);
          }
        },
      });
      break;
    }
    default: {
      console.error("ERROR: Choose a valid option dammit");
    }
  }
};

const logMessage = (topic, partition, text) => {
  console.log(
    `[topic: ${topic}|partition: ${partition}] Print Consumer | ${text}`
  );
};

processConsumer();
