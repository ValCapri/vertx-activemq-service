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
package de.ithempel.vertx.mods.activemq.unit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import de.ithempel.vertx.mods.activemq.MessageConverter;

/**
 * Unit Tests for the {@link MessageConverter} class.
 *
 * @author https://github.com/ithempel[Sebastian Hempel]
 */
public class MessageConverterTest {

    private MessageConverter messageConverter;
    private Session session;

    @Before
    public void setup() throws JMSException {
        session = mock(Session.class);
        when(session.createTextMessage()).then(new Answer<TextMessage>() {
            @Override
            public TextMessage answer(InvocationOnMock invocation) throws Throwable {
                return new ActiveMQTextMessage();
            }
        });
        when(session.createMapMessage()).then(new Answer<MapMessage>() {
            @Override
            public MapMessage answer(InvocationOnMock invocation) throws Throwable {
                return new ActiveMQMapMessage();
            }
        });

        Logger logger = LoggerFactory.getLogger(MessageConverterTest.class);

        messageConverter = new MessageConverter(session, logger);
    }

    @Test
    public void convertJsonObjectStringBodyToTextMessage() {
        String bodyContent = "body content";
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        Message destination = messageConverter.convertToJmsMessage(source);

        assertThat(destination, instanceOf(TextMessage.class));
    }

    @Test
    public void convertJsonObjectJsonBodyToMapMessage() {
        JsonObject bodyContent = new JsonObject();
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        Message destination = messageConverter.convertToJmsMessage(source);

        assertThat(destination, instanceOf(MapMessage.class));
    }

    @Test
    public void convertToMessageUsingJmsSession() throws JMSException {
        String bodyContent = "body content";
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        messageConverter.convertToJmsMessage(source);

        verify(session).createTextMessage();
    }

    @Test
    public void convertJsonObjectStringBodyToTextMessageContent() throws JMSException {
        String bodyContent = "body content";
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        TextMessage destination = (TextMessage) messageConverter.convertToJmsMessage(source);

        assertThat(destination.getText(), equalTo(bodyContent));
    }

    @Test
    public void convertJsonObjectJsonBodyToMapMessageContent() throws JMSException {
        String content = "body content";
        JsonObject bodyContent = new JsonObject();
        bodyContent.put("content", content);
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        MapMessage destination = (MapMessage) messageConverter.convertToJmsMessage(source);

        assertThat(destination.getString("content"), equalTo(content));
    }

    @Test
    public void convertJsonObjectJsonBodyToMapMessageWithAllEntries() throws JMSException {
        JsonObject bodyContent = new JsonObject();
        bodyContent.put("string", "String");
        bodyContent.put("boolean", true);
        bodyContent.put("number", 123);
        JsonObject source = new JsonObject();
        source.put("body", bodyContent);

        MapMessage destination = (MapMessage) messageConverter.convertToJmsMessage(source);

        for (String key : bodyContent.getMap().keySet()) {
            assertThat(destination.itemExists(key), equalTo(true));
        }
    }

    @Test
    public void convertTextMessageToJsonObjectStringBody() throws JMSException {
        TextMessage source = new ActiveMQTextMessage();
        source.setText("");

        JsonObject destination = messageConverter.convertToJsonObject(source);

        assertThat(destination.getString("body"), notNullValue());
    }

    @Test
    public void convertMapMessageToJsonObjectMapBody() {
        MapMessage source = new ActiveMQMapMessage();

        JsonObject destination = messageConverter.convertToJsonObject(source);

        assertThat(destination.getJsonObject("body"), notNullValue());
    }

}
