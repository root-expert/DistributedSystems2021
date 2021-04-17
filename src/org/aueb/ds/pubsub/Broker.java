package org.aueb.ds.pubsub;

import org.aueb.ds.model.Node;

import java.util.ArrayList;

public class Broker extends Node {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();

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
}
