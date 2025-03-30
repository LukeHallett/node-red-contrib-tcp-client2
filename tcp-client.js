/*!
 * Copyright 2024 Jörgen Karlsson
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
'use strict';


module.exports = function (RED) {
    const net = require('net');
    const LogHelper = require('./loghelper');

    class TcpClient {
        constructor(config) {
            RED.nodes.createNode(this, config);
            this.logger = new LogHelper(this, config.debug);
            this.datatype = config.datatype || 'utf8'; // 'utf8', 'base64', 'buffer'
            this.socketTimeout = RED.settings.socketTimeout || 120000;
            this.maxRetries = parseInt(config.maxRetries, 10) || 5;
            this.retryDelay = parseInt(config.retryDelay, 10) || 3000;
            this.delimiter = (config.newline || "").replace("\\n", "\n").replace("\\r", "\r");
            this.stream = (!config.datamode || config.datamode == 'stream'); /* stream, single*/

            if (config.indefiniteRetries) {
                this.maxRetries = Number.MAX_SAFE_INTEGER; // Use MAX_SAFE_INTEGER for practical "indefinite" value
            }
            this.logger.debug("Max retries " + this.maxRetries + ", delay " + this.retryDelay + "ms");
            this.connection = null;
            this.done = null;
            this.logger.info("Init");
            this.msg = null; // for throwing errors mainly;

            this.on('input', (msg, send, done) => {
                let action = RED.util.evaluateNodeProperty(config.action, config.actionType, this, msg);
                this.logger.debug("action: " + action, msg);

                switch (action) {
                    case 'connect':
                    case 'listen': // legacy
                        let host = RED.util.evaluateNodeProperty(config.host, config.hostType, this, msg);
                        let port = RED.util.evaluateNodeProperty(config.port, config.portType, this, msg);
                        this.connect(host, port, msg, done);
                        break;
                    case 'write':
                        let data = RED.util.evaluateNodeProperty(config.write, config.writeType, this, msg);
                        this.write(data, done);
                        break;
                    case 'close':
                        this.close(done);
                        break;
                    default:
                        this.logger.warning(`Unrecognized action: ${action}`);
                        done();
                }
            });


            this.on('close', (done) => {
                // Clear any pending retry timeout
                if (this.connection && this.connection.retryTimeoutId) {
                    clearTimeout(this.connection.retryTimeoutId);
                }
                this._destroySocket();
                this.logger.info("Node is closing. Cleanup done.");
                done(); // Notify Node-RED that cleanup is complete
            });
        }

        doDone() {
            if (this.done && typeof this.done === 'function') {
                this.done(); // Call 'done' to complete the node operation
                this.done = null;
            }
        }

        connect(host, port, msg, done) {
            this.done = done;
            if (this.connection !== null) {
                this.logger.warning(`Connection already exists`);
                this.doDone(); // todo close!?
                return;
            }
            this.connection = { buffer: '', host: host, port: port, retries: 0, attempting: false, retryTimeoutId: null, socket: null };
            this._doConnect(msg);
        }

        _doConnect(msg) {
            this.connection.retryTimeoutId = null;
            this.status({ fill: "yellow", shape: "dot", text: `Connecting to ${this.connection.host}:${this.connection.port}` });
            if (this.connection.socket) {
                this._destroySocket();
            }

            this.connection.socket = net.createConnection(this.connection.port, this.connection.host, () => {
                this.logger.info(`Connected to ${this.connection.host}:${this.connection.port}`);
                this.doDone();
            });
            this._setupSocketEventHandlers(this.connection.socket);
        }

        _destroySocket() {
            if (this.connection && this.connection.socket) {
                this.logger.debug("Destroying old socket");
                this.connection.socket.destroy();
                this.connection.socket = null;
            }
        }

        _setupSocketEventHandlers(socket) {

            socket.on('data', (data) => {
                this.logger.debug("Data!:" + data);
                if (this.stream) {
                    if (this.datatype === 'utf8' || this.datatype === 'base64') {
                        // Convert binary data to the appropriate text format and append to the buffer
                        if (this.connection) {
                            this.connection.buffer += data.toString(this.datatype);

                            // Handle delimited strings for 'utf8' and 'base64'
                            let parts = this.connection.buffer.split(this.delimiter);
                            for (let i = 0; i < parts.length - 1; i++) {
                                let msgPayload = parts[i];
                                /* TODO make this a configurable option
                                if (this.datatype === 'base64') {
                                    // For base64 optionaly convert the payload back to a Buffer
                                    msgPayload = Buffer.from(parts[i], 'base64');
                                }*/
                                let msg = { payload: msgPayload };
                                this.send(msg); // Send each complete message
                                this.logger.debug(`Sent ${this.datatype} data: ${msg.payload}`);
                            }

                            // Keep the last part (incomplete message) in the buffer for the next 'data' event
                            this.connection.buffer = parts[parts.length - 1];
                        } else {
                            // this can happen if streaming and closing, the socket might get data after the close
                            this.logger.debug("Lost connection object");
                        }
                    } else if (this.datatype === 'buffer') {
                        this.send({ payload: data });
                        this.logger.debug("Sent binary data");
                    }
                } else {
                    // If not streaming, send data as single messages based on datatype
                    let msgPayload = data;
                    if (this.datatype === 'utf8') {
                        msgPayload = data.toString('utf8');
                    } else if (this.datatype === 'base64') {
                        msgPayload = data.toString('base64');
                    }
                    let msg = { payload: msgPayload };
                    this.send(msg);
                    this.logger.debug(`Sent ${this.datatype} data as a single message`);
                }
            });


            socket.on('connect', () => {
                this.connection.retries = 0;
                this.logger.debug("Connection established, retries reset.");
                this.status({ fill: "green", shape: "dot", text: `Connected to ${this.connection.host}:${this.connection.port}` });
                this.doDone(); // Complete any pending operation, signaling successful connection
            });

            socket.on('close', () => {
                this.logger.debug(`Socket closed`);
                if (this.connection) {
                    this.logger.debug(`Retrying connection to ${this.connection.host}:${this.connection.port}`);
                    this._retryConnection(this.connection);
                } else {
                    this.logger.debug("Connection already closed when closing socket");
                }
            });

            socket.on('error', (err) => {
                if (this.connection) {
                    this.logger.info(`Socket error for ${this.connection.host}:${this.connection.port}: ${err.message}`);
                    this._destroySocket();
                    this._retryConnection(err);
                } else {
                    this.logger.info("Socket error: " + err.message);
                    // connection gone, probably due to close so we bail out
                    this.status({ fill: "blue", shape: "ring", text: "closed" });
                }
            });
        }

        write(data, done) {
            this.done = done;
            if (this.connection && this.connection.socket) {
                if (!Buffer.ifBuffer(data) && typeof data === "object") {
                    data = JSON.stringify(data) + "\r\n";
                    this.logger.debug("Object converted to string: " + data);
                }
                this.logger.debug("Writing " + data);
                this.connection.socket.write(data, this.datatype, done);
            } else {
                this.logger.warning(`No connection available. Attempting to send data failed.`);
            }
            this.doDone();
        }

        close(done) {
            this.done = done;
            if (this.connection && this.connection.socket) {
                this.connection.socket.end(() => {
                    this.connection = null;
                    this.logger.info(`Connection closed.`);
                });
            }
            this.status({ fill: "blue", shape: "ring", text: "closed" });
            this.doDone();
        }

        _retryConnection(err) {
            if (this.connection && this.connection.retries < this.maxRetries) {
                if (!this.connection.retryTimeoutId) {
                    this.connection.retryTimeoutId = setTimeout(() => {
                        if (this.connection) { // connection might been closed in the meantime 
                            this.connection.retries++;
                            this.logger.info(`Retry ${this.connection.retries}/${this.maxRetries} for ${this.connection.host}:${this.connection.port}`);
                            this._doConnect();
                        }
                    }, this.retryDelay);
                } else {
                    this.logger.debug("Retry already in progress");
                }
            } else {
                const errmsg = `Maximum retries reached for ${this.connection.host}:${this.connection.port}. Giving up. Original error: ${err.message}`;
                this.status({ fill: "red", shape: "ring", text: `Maximum retries reached for ${this.connection.host}:${this.connection.port}.` });
                this._destroySocket();
                this.connection = null;
                this.logger.error(errmsg);
            }
        }

    }
    RED.nodes.registerType("tcp-client", TcpClient);
};
