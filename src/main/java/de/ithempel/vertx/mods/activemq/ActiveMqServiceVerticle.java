/**
 * module-activemq-io
 *
 * Copyright 2014 Sebastian Hempel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ithempel.vertx.mods.activemq;

import de.ithempel.vertx.mods.activemq.impl.ActiveMqServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * Module / Verticle to communicate with an ActiveMQ message broker.
 *
 * The module listens on the event bus for commands to send message to the broker
 * or to subscribe to messages from the broker. The message from / to the module
 * are formated as Json objects. A message to the module must containt the following
 * fields in the Json structure.
 *
 * |===
 * | field              | description
 * |
 * | command            | What command should be executed: send or subscribe.
 * | destination        | Contains the name of the queue to which the message should be send.
 * | subscriber-address | Address on the event bus to which messages from subscribed queue should be send.
 * | body               | The content that should be send to the queue.
 * |===
 *
 * Depending on the format of the body field the following JMS messages are send.
 *
 * |===
 * | body       | JMS MessageType
 * |
 * | String     | TextMessage
 * | JsonObject | MapMessage
 * |===
 *
 * The parameters to connect to the broker can be set when distributing the module.
 * If no parameters are given the verticle connects to a local installed instance
 * of ActiveMQ on the standard wire protocol port 61616 with no authentication.
 *
 * @author https://github.com/ithempel[Sebastian Hempel]
 */
public class ActiveMqServiceVerticle extends AbstractVerticle {

    private ActiveMqService service;
    private Logger logger = LoggerFactory.getLogger(ActiveMqServiceVerticle.class);

    @Override
    public void start() throws Exception {
        super.start();

        // Create the service object
        service = ActiveMqService.create(vertx,config());

        // And register it on the event bus against the configured address
        final String address = config().getString("address");
        if (address == null) {
            throw new IllegalStateException("address field must be specified in config for service verticle");
        }

        ProxyHelper.registerService(ActiveMqService.class, vertx, service, address);

        service.connect();

        String debugMessage = String.format("Listening on EventBus on address %s", address);
        logger.debug(debugMessage);
    }

    @Override
    public void stop() {
        service.disconnect();
    }

    /*@Override
    public void handle(Message<JsonObject> message) {
        String command = getMandatoryString(ActiveMqFieldName.COMMAND.toString(), message);

        switch (command) {
        case "send":
            String destination = getMandatoryString(ActiveMqFieldName.DESTINATION.toString(), message);

            amqClient.send(destination, message.body());
            break;
        case "subscribe":
            String subscribeDestination = getMandatoryString(ActiveMqFieldName.DESTINATION.toString(), message);
            final String subscriberAddress =
                    getMandatoryString(ActiveMqFieldName.SUBSCRIBER_ADDRESS.toString(), message);

            amqClient.subscribe(subscribeDestination, new Handler<JsonObject>() {
                @Override
                public void handle(JsonObject messageReceived) {
                    eb.send(subscriberAddress, messageReceived);
                }
            });
            break;
        default:
            String infoMessage = String.format("Cannot handle command '%s'", command);
            logger.info(infoMessage);
            break;
        }
    }
    */

}
