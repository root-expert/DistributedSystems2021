package org.aueb.ds.pubsub;

import java.io.Serializable;
import java.util.ArrayList;

public class Broker implements Serializable {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();

    protected String hash = null;
    protected String ip = null;
    protected int port;

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
