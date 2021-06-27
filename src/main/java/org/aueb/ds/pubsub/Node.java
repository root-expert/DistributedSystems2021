package org.aueb.ds.pubsub;

import org.aueb.ds.model.pubsub.Connection;

import java.io.IOException;
import java.util.ArrayList;

public interface Node {

    boolean init();

    ArrayList<Broker> getBrokers();

    Connection connect(String ip, int port) throws IOException;

    void disconnect(Connection connection);

    void updateNodes();
}
