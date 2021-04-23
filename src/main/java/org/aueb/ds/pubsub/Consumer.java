package org.aueb.ds.pubsub;

import org.aueb.ds.model.Value;

import java.net.Socket;
import java.util.ArrayList;

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
    public Socket connect(String ip, int port) {
        Socket socket =  super.connect(ip, port);

        // Send messages to broker
        return null;
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    @Override
    public void run() {

    }
}
