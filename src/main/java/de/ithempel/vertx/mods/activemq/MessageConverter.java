/**
 * mod-activemq-io
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

import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;

/**
 * The MessageConverter is used to convert JsonObject instances from Vert.x into JMS Messages
 * and the other way round.
 *
 * TODO:
 *  * Find a way to convert the imortant header values from / to JsonObject
 *
 * @author https://github.com/ithempel[Sebastian Hempel]
 */
public class MessageConverter {

    private final Session session;
    private final Logger logger;

    public MessageConverter(Session session, Logger logger) {
        this.session = session;
        this.logger = logger;
    }

    public Message convertToJmsMessage(String textContent) throws RuntimeException {
        Message message = null;

        try {
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText(textContent);

            message = textMessage;
        }
        catch (JMSException e) {
            String errorMessage = "JMSException while converting String to JMS Message";
            throw new RuntimeException(errorMessage,e);
        }

        return message;
    }

    public Message convertToJmsMessage(JsonObject jsonContent) throws RuntimeException {
        Message message = null;

        try {
            Map<String, Object> mapContent = jsonContent.getMap();

            MapMessage mapMessage = session.createMapMessage();
            for (Entry<String, Object> entry : mapContent.entrySet()) {
                mapMessage.setObject(entry.getKey(), entry.getValue());
            }

            message = mapMessage;
        }
        catch (JMSException e) {
            String errorMessage = "JMSException while converting JsonObject to JMS Message";
            throw new RuntimeException(errorMessage, e);
        }

        return message;
    }

    public JsonObject convertToJsonObject(Message source) {
        JsonObject json = new JsonObject();

        try {
            if (source instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) source;

                json.put(ActiveMqFieldName.BODY.toString(), textMessage.getText());
            }
            else if (source instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) source;

                JsonObject bodyContent = new JsonObject();
                Enumeration<String> mapNames = mapMessage.getMapNames();
                while (mapNames.hasMoreElements()) {
                    String fieldName = mapNames.nextElement();

                    bodyContent.put(fieldName, mapMessage.getObject(fieldName));
                }

                json.put(ActiveMqFieldName.BODY.toString(), bodyContent);
            }
            else {
                String messageType = source.getClass().getName();
                String errorMessage = String.format(
                        "unkown JMS Message type %s could not be converted to a JsonObject", messageType);
                logger.error(errorMessage);
            }
        }
        catch (JMSException e) {
            String errorMessage = "JMSException while converting JMS Message to JsonObject";
            logger.error(errorMessage, e);
        }

        return json;
    }

}
