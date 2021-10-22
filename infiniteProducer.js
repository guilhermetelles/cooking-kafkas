const { Kafka } = require("kafkajs");

run().then(
  () => console.log("Done"),
  (err) => console.log(err)
);

const topicName = "cooked-kafkas";
let count = 0;

async function run() {
  const kafka = new Kafka({ brokers: ["localhost:9092"] });

  const producer = kafka.producer();
  await producer.connect();

  while (true) {
    randomNumber = Math.floor(Math.random() * 101);
    count++;
    console.log(`count: ${count} | randomNumber: ${randomNumber}`);
    await producer.send({
      topic: topicName,
      messages: [
        {
          key: "randomNumber",
          value: randomNumber.toString(),
        },
        {
          key: "count",
          value: count.toString(),
        },
      ],
    });
  }
}
