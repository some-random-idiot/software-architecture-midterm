const express = require("express");
const path = require("path");
const http = require("http");
const amqp = require('amqplib');

const RABBIT = process.env.RABBIT;

//
// Setup event handlers.
//
function setupHandlers(app, messageChannel) {
    admsg = null
    function consumeAdMessage(msg) { // Handler for coming messages.
        const parsedMsg = JSON.parse(msg.content.toString()); // Parse the JSON message.
        if (parsedMsg.content !== "request")
        {
            console.log("Received a 'ad' message");
            admsg = parsedMsg.content
            return messageChannel.ack(msg);
        }
    }

    app.set("views", path.join(__dirname, "views")); // Set directory that contains templates for views.
    app.set("view engine", "hbs"); // Use hbs as the view engine for Express.
    
    app.use(express.static("public"));

    //
    // Main web page that lists videos.
    //
    app.get("/", (req, res) => {
        http.request( // Get the list of videos from the metadata microservice.
            {
                host: `metadata`,
                path: `/videos`,
                method: `GET`,
            },
            (response) => {
                let data = "";
                response.on("data", chunk => {
                    data += chunk;
                });

                response.on("end", () => {
                    // Renders the video list for display in the browser.
                    broadcastAdMessage(messageChannel, "request");
                    res.render("video-list", { videos: JSON.parse(data).videos, admsg: admsg });
                });

                response.on("error", err => {
                    console.error("Failed to get video list.");
                    console.error(err || `Status code: ${response.statusCode}`);
                    res.sendStatus(500);
                });
            }
        ).end();
    });

    //
    // Web page to play a particular video.
    //
    app.get("/video", (req, res) => {
        const videoId = req.query.id;
        http.request( // Get a particular video from the metadata microservice.
            {
                host: `metadata`,
                path: `/video?id=${videoId}`,
                method: `GET`,
            },
            (response) => {
                let data = "";
                response.on("data", chunk => {
                    data += chunk;
                });

                response.on("end", () => {
                    const metadata = JSON.parse(data).video;
                    const video = {
                        metadata,
                        url: `/api/video?id=${videoId}`,
                    };
                    
                    // Renders the video for display in the browser.
                    res.render("play-video", { video });
                });

                response.on("error", err => {
                    console.error(`Failed to get details for video ${videoId}.`);
                    console.error(err || `Status code: ${response.statusCode}`);
                    res.sendStatus(500);
                });
            }
        ).end();
    });

    //
    // Web page to upload a new video.
    //
    app.get("/upload", (req, res) => {
        broadcastAdMessage(messageChannel, "request");
        res.render("upload-video", { admsg: admsg });
    });

    //
    // Web page to show the users viewing history.
    //
    app.get("/history", (req, res) => {
        http.request( // Gets the viewing history from the history microservice.
            {
                host: `history`,
                path: `/videos`,
                method: `GET`,
            },
            (response) => {
                let data = "";
                response.on("data", chunk => {
                    data += chunk;
                });

                response.on("end", () => {
                    // Renders the history for display in the browser.
                    broadcastAdMessage(messageChannel, "request");
                    res.render("history", { videos: JSON.parse(data).videos, admsg: admsg });
                });

                response.on("error", err => {
                    console.error("Failed to get history.");
                    console.error(err || `Status code: ${response.statusCode}`);
                    res.sendStatus(500);
                });
            }
        ).end();
    });

    //
    // HTTP GET API to stream video to the user's browser.
    //
    app.get("/api/video", (req, res) => {
        
        const forwardRequest = http.request( // Forward the request to the video streaming microservice.
            {
                host: `video-streaming`,
                path: `/video?id=${req.query.id}`,
                method: 'GET',
            }, 
            forwardResponse => {
                res.writeHeader(forwardResponse.statusCode, forwardResponse.headers);
                forwardResponse.pipe(res);
            }
        );
        
        req.pipe(forwardRequest);
    });

    //
    // HTTP POST API to upload video from the user's browser.
    //
    app.post("/api/upload", (req, res) => {

        const forwardRequest = http.request( // Forward the request to the video streaming microservice.
            {
                host: `video-upload`,
                path: `/upload`,
                method: 'POST',
                headers: req.headers,
            }, 
            forwardResponse => {
                res.writeHeader(forwardResponse.statusCode, forwardResponse.headers);
                forwardResponse.pipe(res);
            }
        );
        
        req.pipe(forwardRequest);
    });

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
function startHttpServer(messageChannel) {
    return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
        const app = express();
        setupHandlers(app, messageChannel);

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
    return connectRabbit()                          // Connect to RabbitMQ...
        .then(messageChannel => {                   // then...
            return startHttpServer(messageChannel); // start the HTTP server.
        });
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });

function connectRabbit() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
        .then(connection => {
            console.log("Connected to RabbitMQ.");

            return connection.createChannel() // Create a RabbitMQ messaging channel.
                .then(messageChannel => {
                    return messageChannel.assertExchange("ad", "fanout") // Assert that we have a "viewed" exchange.
                        .then(() => {
                            return messageChannel;
                        });
                });
        });
}

function broadcastAdMessage(messageChannel, content) {
    console.log(`Publishing message on "ad" exchange.`);

    const msg = { content: content }
    const jsonMsg = JSON.stringify(msg);
    messageChannel.publish("ad", "", Buffer.from(jsonMsg)); // Publish message to the "ad" exchange.
}