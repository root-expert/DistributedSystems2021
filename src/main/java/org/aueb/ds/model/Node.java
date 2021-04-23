package org.aueb.ds.model;

import org.aueb.ds.pubsub.Broker;

import java.net.Socket;
import java.util.ArrayList;

public interface Node {

    void init(String ip, int port);

    ArrayList<Broker> getBrokers();

    Socket connect(String ip, int port);

    void disconnect();

    void updateNodes();
}
