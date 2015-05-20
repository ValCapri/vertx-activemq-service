package de.ithempel.vertx.mods.activemq;

import de.ithempel.vertx.mods.activemq.impl.ActiveMqServiceImpl;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * Created by lionelschinckus on 20/05/15.
 */
@VertxGen
@ProxyGen
public interface ActiveMqService {
    static ActiveMqService create(Vertx vertx, JsonObject config) {
        JsonObject activeMqConfig = config.getJsonObject("activemq", new JsonObject());

        return new ActiveMqServiceImpl(vertx, activeMqConfig);
    }

    static ActiveMqService createEventBusProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(ActiveMqService.class, vertx, address);
    }

    @ProxyIgnore
    void connect();

    @ProxyIgnore
    void disconnect();

    void sendString(String destination, String message, Handler<AsyncResult<JsonObject>> asyncResultHandler);
    void send(String destination, JsonObject message, Handler<AsyncResult<JsonObject>> asyncResultHandler);

    void subscribe(String destination, Handler<AsyncResult<JsonObject>> subscriberHandler);
}
