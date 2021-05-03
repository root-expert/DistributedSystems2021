package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;
import org.aueb.ds.model.config.BrokerConfig;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Broker implements Node, Serializable, Runnable {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();

    private HashMap<Publisher, List<String>> publisherAssociatedHashtags = new HashMap<>();
    private HashMap<Broker, List<String>> brokerAssociatedHashtags = new HashMap<>();

    protected BrokerConfig config;
    protected String hash = null;

    public Broker(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public void init() {

    }

    public void calculateKeys() {

    }

    public Publisher acceptConnection(Publisher publisher) {
        return null;
    }

    public Consumer acceptConnection(Consumer consumer) {
        return null;
    }

    public void notifyPublisher(String topic) {

    }

    public void notifyBrokersOnChanges() {

    }

    public void pull(String topic) {

    }

    public void filterConsumers(String consumer) {

    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return null;
    }

    @Override
    public Connection connect(String ip, int port) {
        return null;
    }

    @Override
    public void disconnect(Connection connection) {

    }

    @Override
    public void updateNodes() {

    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(config.getPort());

            while (true) {
                Socket socket = serverSocket.accept();
                Thread handler = new Thread(new Handler(socket, this));
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class Handler implements Runnable {
        private Socket socket;
        private Broker broker;

        public Handler(Socket socket, Broker broker) {
            this.socket = socket;
            this.broker = broker;
        }

        @Override
        public void run() {
            // Handle Broker, Publisher, Consumer requests
        }
    }
}
