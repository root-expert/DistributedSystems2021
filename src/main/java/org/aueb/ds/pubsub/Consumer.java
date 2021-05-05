package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class Consumer extends AppNode implements Runnable {

    public Consumer(AppNodeConfig conf) {
        super(conf);
    }

    /**
     * Initializes Consumer's state. Connects to the first Broker
     * retrieving the rest of the Brokers.
     */
    @Override
    public void init() {
        Connection connection = connect(config.getBrokerIP(), config.getBrokerPort());

        try {
            connection.in = new ObjectInputStream(connection.socket.getInputStream());
            connection.out = new ObjectOutputStream(connection.socket.getOutputStream());

            connection.out.writeUTF("getBrokerList");
            connection.out.flush();

            this.setBrokers((ArrayList<Broker>) connection.in.readObject());
            System.out.println("Received broker list");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            disconnect(connection);
        }
    }

    /**
     * Subscribe the consumer to the specified topic.
     *
     * @param broker The broker to subscribe.
     * @param topic  The topic to be subscribed on.
     */
    public void subscribe(Broker broker, String topic) {
        Connection connection = super.connect(broker.config.getIp(), broker.config.getPort());

        try {
            connection.in = new ObjectInputStream(connection.socket.getInputStream());
            connection.out = new ObjectOutputStream(connection.socket.getOutputStream());

            connection.out.writeUTF("subscribe");
            connection.out.writeUTF(topic);
            connection.out.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    /**
     * Unsubscribe the consumer from the specified topic.
     *
     * @param broker The broker to which is currently subscribed.
     * @param topic  The topic to unsubscribe from.
     */
    public void unsubscribe(Broker broker, String topic) {
        Connection connection = super.connect(broker.config.getIp(), broker.config.getPort());

        try {
            connection.in = new ObjectInputStream(connection.socket.getInputStream());
            connection.out = new ObjectOutputStream(connection.socket.getOutputStream());

            connection.out.writeUTF("unsubscribe");
            connection.out.writeUTF(topic);
            connection.out.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    public void playData(String topic, Value value) {

    }

    /**
     * Registers a Consumer to the specified Broker.
     *
     * @param ip   The IP to open the connection to.
     * @param port The port to open the connection to.
     * @return A Connection object.
     */
    @Override
    public Connection connect(String ip, int port) {
        // Open Socket connection with the Broker
        Connection connection = super.connect(ip, port);

        try {
            connection.out.writeUTF("register");
            connection.out.writeObject(this);
            connection.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return connection;
    }

    /**
     * Unregisters the Consumer from the specified Broker.
     *
     * @param connection The Connection object to communicate with the broker
     */
    @Override
    public void disconnect(Connection connection) {
        super.disconnect(connection);
    }

    @Override
    public void run() {

    }
}
