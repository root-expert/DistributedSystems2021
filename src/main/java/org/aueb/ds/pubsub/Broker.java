package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;

import java.io.Serializable;
import java.util.ArrayList;

public class Broker implements Node, Serializable {

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

    @Override
    public void init(String ip, int port) {

    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return null;
    }

    @Override
    public Connection connect(String ip, int port) {
        return null;
    }

    @Override
    public void disconnect(Connection connection) {

    }

    @Override
    public void updateNodes() {

    }
}
