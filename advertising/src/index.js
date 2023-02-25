const express = require("express");
const mongodb = require("mongodb");
const amqp = require('amqplib');
const bodyParser = require("body-parser");

if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.");
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
const RABBIT = process.env.RABBIT;

//
// Connect to the database.
//
function connectDb() {
    return mongodb.MongoClient.connect(DBHOST) 
        .then(client => {
            return client.db(DBNAME);
        });
}

//
// Connect to the RabbitMQ server.
//
function connectRabbit() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
        .then(messagingConnection => {
            console.log("Connected to RabbitMQ.");

            return messagingConnection.createChannel(); // Create a RabbitMQ messaging channel.
        });
}

//
// Setup event handlers.
//
function setupHandlers(app, db, messageChannel) {

    const productsCollection = db.collection("products");

    function consumeAdMessage(msg) { // Handler for coming messages.
        const parsedMsg = JSON.parse(msg.content.toString()); // Parse the JSON message.
        if (parsedMsg.content === "request")
        {
            console.log("Received a 'ad' message");
            broadcastAdMessage(messageChannel, "Test ad message");
            return messageChannel.ack(msg);
        }
    }

    return messageChannel.assertExchange("ad", "fanout") // Assert that we have an "ad" exchange.
        .then(() => {
            return messageChannel.assertQueue("", {});
        })
        .then(response => {
            const queueName = response.queue;
            console.log(`Created queue ${queueName}, binding it to "ad" exchange.`);
            return messageChannel.bindQueue(queueName, "ad", "") // Bind the queue to the exchange.
                .then(() => {
                    return messageChannel.consume(queueName, consumeAdMessage); // Start receiving messages from the anonymous queue.
                });
        });
}

//
// Start the HTTP server.
//
function startHttpServer(db, messageChannel) {
    return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
        const app = express();
        app.use(bodyParser.json()); // Enable JSON body for HTTP requests.
        setupHandlers(app, db, messageChannel);

        const port = process.env.PORT && parseInt(process.env.PORT) || 3000;
        app.listen(port, () => {
            resolve(); // HTTP server is listening, resolve the promise.
        });
    });
}

//
// Application entry point.
//
function main() {
    return connectDb()                                          // Connect to the database...
        .then(db => {                                           // then...
            return connectRabbit()                              // connect to RabbitMQ...
                .then(messageChannel => {                       // then...
                    return startHttpServer(db, messageChannel); // start the HTTP server.
                });
        });
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });

function broadcastAdMessage(messageChannel, content) {
    console.log(`Publishing message on "ad" exchange.`);

    const msg = { content: content }
    const jsonMsg = JSON.stringify(msg);
    messageChannel.publish("ad", "", Buffer.from(jsonMsg)); // Publish message to the "ad" exchange.
}