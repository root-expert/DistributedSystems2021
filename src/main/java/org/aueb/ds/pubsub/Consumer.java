package org.aueb.ds.pubsub;

import org.aueb.ds.model.Node;
import org.aueb.ds.model.Value;

import java.util.ArrayList;

public class Consumer implements Node {

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
    public ArrayList<Broker> getBrokers() {
        return null;
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }
}
