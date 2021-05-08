package org.aueb.ds.model;

import org.aueb.ds.pubsub.Broker;

import java.io.IOException;
import java.util.ArrayList;

public interface Node {

    void init();

    ArrayList<Broker> getBrokers();

    Connection connect(String ip, int port) throws IOException;

    void disconnect(Connection connection);

    void updateNodes();
}
