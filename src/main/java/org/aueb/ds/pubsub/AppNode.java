package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class AppNode implements Node {

    private static ArrayList<Broker> brokers = new ArrayList<>();

    protected String ip;
    protected int port;
    protected final int BROKER_PORT = 23456;

    @Override
    public void init(String ip, int port) {

    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return brokers;
    }

    public void setBrokers(ArrayList<Broker> brokers) {
        AppNode.brokers = brokers;
    }

    @Override
    public Connection connect(String ip, int port) {
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            socket = new Socket(ip, port);
            in = new ObjectInputStream(socket.getInputStream());
            out = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Connection(socket, in, out);
    }

    @Override
    public void disconnect(Connection connection) {
        try {
            connection.in.close();
            connection.out.close();
            connection.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateNodes() {

    }
}
