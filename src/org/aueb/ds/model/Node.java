package org.aueb.ds.model;

import org.aueb.ds.pubsub.Broker;

import java.util.ArrayList;

public interface Node {

    ArrayList<Broker> brokers = new ArrayList<>();

    ArrayList<Broker> getBrokers();

    void connect();

    void disconnect();

    void updateNodes();
}
