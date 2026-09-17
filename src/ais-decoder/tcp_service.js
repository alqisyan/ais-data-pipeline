const net = require("net");
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "ais-decoder",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    retry: {
        retries: 10,
        initialRetryTime: 300
    }
});

const producer = kafka.producer();

const messageQueue = [];
let isProcessingQueue = false;

//Send AIS Data to Kafka
async function processQueue() {
    if (isProcessingQueue || messageQueue.length === 0) return;

    isProcessingQueue = true;

    while (messageQueue.length > 0) {
        const payload = messageQueue.shift();
        const dataParser = JSON.parse(JSON.stringify(payload, null, 2));
        try {
            await producer.send({
                topic: "ais-raw",
                messages: [
                    {
                        key: String(dataParser?.mainCalculator?.mmsi) || "unknown-sensor",
                        value: JSON.stringify(payload),
                    },
                ],
            });
            console.log(`[Kafka Success] Send Ships Data with MMSI: ${dataParser?.mainCalculator?.mmsi}`);
        } catch (err) {
            console.error("[Kafka Error] Failed to send data:", err);
            messageQueue.unshift(payload);
            break;
        }
    }

    isProcessingQueue = false;
}

//Listen to Custom AIS Decoder
async function startTcpService() {
    try {
        console.log("Connecting to Apache Kafka...");
        await producer.connect();
        console.log("Kafka Producer connected.");
        const client = net.createConnection({ port: 8040 }, () => {
            console.log('Connected to AIS Decoder');
        });

        client.on('data', (data) => {
            console.log('Received Data:', data.toString());
            if (data) {
                messageQueue.push(data);
                processQueue();
            }
        });

        client.on('end', () => {
            console.log('Disconnected from server');
        });
    } catch (error) {
        console.error("Failed to start TCP Service:", error);
        process.exit(1);
    }
}

const gracefulShutdown = async () => {
    console.log("\nDisconnecting...");
    try {
        await producer.disconnect();
        console.log("Disconnected from Kafka");
    } catch (e) {
        console.error("Failed to disconnect from Kafka", e);
    } finally {
        process.exit(0);
    }
};

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

startTcpService()