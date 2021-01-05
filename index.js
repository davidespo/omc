require('dotenv').config();
const { nanoid } = require('nanoid');
const { Kafka } = require('kafkajs');
const AivenApi = require('@de44/aiven-node');

const token = process.env.TOKEN || '';
const projectId = process.env.PROJECT_ID || '';
const kafkaServiceName = process.env.KAFKA_SERVICE_NAME || '';
const topic = process.env.KAFKA_TOPIC || '';
const producerCount = process.env.PRODUCER_COUNT;

const avn = new AivenApi({ token, projectId });

const messages = Array(100).fill({ value: nanoid() });
const payload = { topic, messages };

async function startProducer(/** @type {Kafka} */ kafka) {
  const id = nanoid();
  console.log(`Initializing producer ${id}`);
  const producer = kafka.producer();
  const delay = Math.random() * 10000;
  console.log(`Starting ${id} in ${delay}ms`);
  setTimeout(async () => {
    console.log(`Connecting producer ${id}`);
    console.log(`Starting producer ${id}`);
    await producer.connect();
    while (true) {
      await producer.send(payload);
    }
  }, delay);
}

async function run() {
  const creds = await avn.getKafkaCreds(kafkaServiceName);
  const { conn } = creds;
  console.log(`Have creds for ${creds.kafkaUri}`);
  const kafka = new Kafka(conn);
  for (let i = 0; i < producerCount; i++) {
    startProducer(kafka);
  }
}

run();
