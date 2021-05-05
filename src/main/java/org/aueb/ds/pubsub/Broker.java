package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.BrokerConfig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Broker implements Node, Serializable, Runnable {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();

    private HashMap<Publisher, List<String>> publisherAssociatedHashtags = new HashMap<>();
    private HashMap<Broker, List<String>> brokerAssociatedHashtags = new HashMap<>();
    private HashMap<String, ArrayList<Value>> videoList = new HashMap<>();

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
        // if at least one publisher is related to the topic
        Connection connection = null;
        /**
         * For every publisher that is affiliated with the broker, either if the channel
         * name matches the topic or is a hashtag that the Publisher has content of
         */
        for (Publisher pu : publisherAssociatedHashtags.keySet()) {
            if (publisherAssociatedHashtags.get(pu).contains(topic) || pu.getChannelName().channelName.equals(topic)) {
                try {
                    connection = this.connect(pu.config.getIp(), pu.config.getPublisherPort());
                    connection.out.writeUTF("notify");
                    connection.out.writeUTF(topic);
                    connection.out.flush();
                    int exitCode = connection.in.readInt();
                    if (exitCode == 0) {
                        // TODO: Change pull paqrameters
                        pull(topic);
                    }
                    disconnect(connection);
                } catch (IOException io) {
                    System.out.println("Error: there was problem in input/output: " + io.getMessage());
                }
            }
        }
    }

    public void notifyBrokersOnChanges() {

    }

    /**
     * Pulls data of a specified topic
     *
     * @param topic The channelName or hashTag to pull data
     */
    public void pull(Publisher publisher, String topic) {
        ArrayList<Value> chunkList = new ArrayList<>();

        Connection connection = connect(publisher.config.getIp(), publisher.config.getPublisherPort());
        try {
            connection.out.writeUTF("push");
            connection.out.writeUTF(topic);
            connection.out.flush();

            if (connection.in.readInt() == 0) {
                int numOfVideos = connection.in.readInt();
                for (int i = 1; i <= numOfVideos; i++) {
                    int numOfChunks = connection.in.readInt();
                    chunkList.clear();
                    for (int j = 1; j <= numOfChunks; j++) {
                        chunkList.add((Value) connection.in.readObject());
                    }
                    Collections.sort(chunkList);
                    videoList.put(topic, chunkList);
                }
            } else {
                return;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            disconnect(connection);
        }
    }

    public void filterConsumers(String consumer) {

    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return null;
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
            if(connection.in!=null)
                connection.in.close();
            if(connection.out!=null)
            connection.out.close();
            if(connection.socket!=null)
            connection.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
