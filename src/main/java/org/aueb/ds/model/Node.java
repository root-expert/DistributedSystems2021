package org.aueb.ds.model;

import org.aueb.ds.pubsub.Broker;

import java.util.ArrayList;

public interface Node {

    void init();

    ArrayList<Broker> getBrokers();

    Connection connect(String ip, int port);

    void disconnect(Connection connection);

    void updateNodes();
}
