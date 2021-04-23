package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class AppNode implements Node {
    
    private ArrayList<Broker> brokers = new ArrayList<>();

    @Override
    public void init(String ip, int port) {
        Connection connection = connect(ip, port);

        try {
            connection.in = new ObjectInputStream(connection.socket.getInputStream());
            connection.out = new ObjectOutputStream(connection.socket.getOutputStream());

            connection.out.writeUTF("getBrokerList");
            connection.out.flush();

            brokers = (ArrayList<Broker>) connection.in.readObject();
            System.out.println("Received broker list");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return brokers;
    }

    @Override
    public Connection connect(String ip, int port) {
        Socket socket = null;

        try {
            socket = new Socket(ip, port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Connection(socket);
    }

    @Override
    public void disconnect(Connection connection) {

    }

    @Override
    public void updateNodes() {

    }
}
