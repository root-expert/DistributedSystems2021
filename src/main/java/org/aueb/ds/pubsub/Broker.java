package org.aueb.ds.pubsub;

import org.aueb.ds.model.Node;

import java.io.Serializable;
import java.util.ArrayList;

public class Broker implements Node, Serializable {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();

    protected String hash = null;

    public void calculateKeys() {

    }

    public Publisher acceptConnection(Publisher publisher) {
        return null;
    }

    public Consumer acceptConnection(Consumer consumer) {
        return null;
    }

    public void notifyPublisher(String topic) {

    }

    public void notifyBrokersOnChanges() {

    }

    public void pull(String topic) {

    }

    public void filterConsumers(String consumer) {

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
