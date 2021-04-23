package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;

public class Consumer extends AppNode implements Runnable {

    /**
     * Registers a consumer
     * @param broker The broker to be registered on.
     * @param topic The topic to be registered on.
     */
    public void register(Broker broker, String topic) {

    }

    public void disconnect(Broker broker, String topic) {

    }

    public void playData(String topic, Value value) {

    }

    @Override
    public Connection connect(String ip, int port) {
        Connection connection =  super.connect(ip, port);

        // Send messages to broker
        return null;
    }

    @Override
    public void disconnect(Connection connection) {
        super.disconnect(connection);
    }

    @Override
    public void run() {

    }
}
