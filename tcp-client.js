'use strict';

module.exports = function (RED) {

    var socketTimeout = RED.settings.socketTimeout||null;

    function TcpClient(config) {

        var net = require('net'); //https://nodejs.org/api/net.html
        var crypto = require('crypto');

        RED.nodes.createNode(this, config);

        this.action = config.action || "listen"; /* listen,close,write */
        this.host = config.host || null;
        this.port = config.port * 1;
        this.topic = config.topic;
        this.debug = config.debug;
        this.stream = (!config.datamode || config.datamode=='stream'); /* stream,single*/
        this.datatype = config.datatype || 'buffer'; /* buffer,utf8,base64 */
        this.newline = (config.newline || "").replace("\\n","\n").replace("\\r","\r");

        var node = this;

        var connectionPool = {};
        var server;

        node.on('input', function (msg, nodeSend, nodeDone) {

            if (config.actionType === 'msg' || config.actionType === 'flow' || config.actionType === 'global') {
                node.action = RED.util.evaluateNodeProperty(config.action, config.actionType, this, msg);
            }

            if (config.hostType === 'msg' || config.hostType === 'flow' || config.hostType === 'global') {
                node.host = RED.util.evaluateNodeProperty(config.host, config.hostType, this, msg);
            }

            if (config.portType === 'msg' || config.portType === 'flow' || config.portType === 'global') {
                node.port = (RED.util.evaluateNodeProperty(config.port, config.portType, this, msg)) * 1;
            }

            var id = crypto.createHash('md5').update(`${node.host}${node.port}`).digest("hex");

            var configure = (id) => {

                var socket = connectionPool[id].socket;

                socket.setKeepAlive(true, 120000);

                if (socketTimeout !== null) {
                    socket.setTimeout(socketTimeout);
                }

                socket.on('data', (data) => {

                    if (node.debug === 'all') node.warn(`Data received from ${socket.remoteAddress}:${socket.remotePort}`);

                    if (node.datatype != 'buffer') {
                        data = data.toString(node.datatype);
                    }
    
                    var buffer = connectionPool[id].buffer;
    
                    if (node.stream) {
    
                        var result = {
                            topic: msg.topic || config.topic
                        };
    
                        if ((typeof data) === "string" && node.newline !== "") {
    
                            buffer = buffer + data;
                            var parts = buffer.split(node.newline);
    
                            for (var i = 0; i < parts.length - 1; i += 1) {
                                
                                result.payload = parts[i];
                                nodeSend(result);
    
                            }
    
                            buffer = parts[parts.length - 1];
    
                        }
                        else {
                            result.payload = data;
                            nodeSend(result);
                        }
    
                    }
                    else {
    
                        if ((typeof data) === "string") {
                            buffer = buffer + data;
                        }
                        else {
                            buffer = Buffer.concat([buffer, data], buffer.length + data.length);
                        }
    
                    }
    
                    connectionPool[id].buffer = buffer;

                });

                socket.on('end', function () {
                    if (!node.stream || (node.datatype === "utf8" && node.newline !== "")) {
                        var buffer = connectionPool[id].buffer;
                        if (buffer.length > 0) nodeSend({ topic: msg.topic || config.topic, payload: buffer });
                        connectionPool[id].buffer = null;
                    }
                });

                socket.on('timeout', function () {
                    socket.end();
                });

                socket.on('close', function () {
                    delete connectionPool[id];
                });

                socket.on('error', function (err) {
                    node.log(err);
                });

            };

            var close = function() {

                if (node.debug === 'all') node.warn(`Closing connection to ${node.host}:${node.port}`);
                let socket = connectionPool[id].socket;
                // Properly close the socket
                socket.end(() => {
                    if (node.debug === 'all') node.warn(`Successfully closed connection to ${node.host}:${node.port}`);
                });
                socket.destroy(); // Ensures the connection is fully closed
            };

            var listen = function() {
                
                if (typeof connectionPool[id] === 'undefined') {

                        connectionPool[id] = {
                            socket: net.connect(node.port, node.host),
                            buffer: (node.datatype == 'buffer') ? Buffer.alloc(0) : ""
                        };
                        configure(id);
                    }
            };

            var write = function() {
                if (connectionPool[id] == null) return;
                var socket = connectionPool[id].socket;
                var writeMsg = config.write;

                if (config.writeType === 'msg' || config.writeType === 'flow' || config.writeType === 'global') {
                    writeMsg = RED.util.evaluateNodeProperty(config.write, config.writeType, this, msg);
                }

                if (writeMsg == null) return;

                if (Buffer.isBuffer(writeMsg)) {
                    socket.write(writeMsg);
                } else if (typeof writeMsg === "string" && node.datatype == 'base64') {
                    socket.write(Buffer.from(writeMsg, 'base64'));
                } else {
                    socket.write(Buffer.from("" + writeMsg));
                }

            };
            if (node.debug === 'all') node.warn(`Action:  ${node.action}`);
            switch (node.action.toLowerCase()) {
                case 'close':
                    close();
                    break;
                case 'write':
                    write();
                    break;
                default:
                    listen();
            }

        });

        node.on("close",function() {

            for (var c in connectionPool) {
                if (connectionPool.hasOwnProperty(c)) {
                    var socket = connectionPool[c].socket;
                    socket.end();
                    socket.destroy();
                    socket.unref();
                }
            }

            server.close();
            connectionPool = {};
            node.status({});

        });

    };

    RED.nodes.registerType("tcp-client", TcpClient);

};