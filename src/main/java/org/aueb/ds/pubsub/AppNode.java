package org.aueb.ds.pubsub;

import org.aueb.ds.model.pubsub.Connection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class AppNode implements Node {

    private static ArrayList<Broker> brokers = new ArrayList<>();

    public AppNode() {
    }

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return brokers;
    }

    public void setBrokers(ArrayList<Broker> brokers) {
        AppNode.brokers = brokers;
    }

    @Override
    public Connection connect(String ip, int port) throws IOException {
        Socket socket;
        ObjectInputStream in;
        ObjectOutputStream out;

        socket = new Socket(ip, port);
        in = new ObjectInputStream(socket.getInputStream());
        out = new ObjectOutputStream(socket.getOutputStream());

        return new Connection(socket, in, out);
    }

    @Override
    public void disconnect(Connection connection) {
        if (connection == null) return;

        try {
            connection.out.writeUTF("end");
            connection.out.flush();

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
