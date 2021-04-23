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
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            socket = new Socket(ip, port);
            in = new ObjectInputStream(socket.getInputStream());
            out = new ObjectOutputStream(socket.getOutputStream());

            out.writeUTF("getBrokerList");
            out.flush();

            brokers = (ArrayList<Broker>) in.readObject();
            System.out.println("Received broker list");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) out.close();
                if (in != null) in.close();
                if (socket != null) socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
