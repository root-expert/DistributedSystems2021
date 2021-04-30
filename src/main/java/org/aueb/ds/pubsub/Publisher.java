package org.aueb.ds.pubsub;

import org.aueb.ds.model.ChannelName;
import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.util.Hashing;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

public class Publisher extends AppNode implements Runnable {

    private ChannelName channelName = new ChannelName(UUID.randomUUID().toString()); // TODO: Change me

    public Publisher(AppNodeConfig conf) {
        super(conf);
    }

    /**
     * Add hashtag to ChannelName's List ("#viral")
     * Inform Brokers
     *
     * @param hashtag HashTag added.
     */
    public void addHashTag(String hashtag) {
        channelName.hashtagsPublished.add(hashtag);
        notifyBrokersForHashTags(hashtag, true);
        System.out.println("Hashtag added.");
    }

    /**
     * Remove hashtag from ChannelName's List ("#viral")
     * Inform Brokers
     *
     * @param hashtag HashTag removed.
     */
    public void removeHashTag(String hashtag) {
        channelName.hashtagsPublished.remove(hashtag);
        notifyBrokersForHashTags(hashtag, false);
        System.out.println("Hashtag removed.");
    }

    public void getBrokerList() {

    }

    /**
     * Hashes the specified topic with MD5 algo.
     *
     * @param topic Topic to hash.
     * @return The broker which is responsible for the specified topic.
     */
    public Broker hashTopic(String topic) {
        Hashing hashing = new Hashing();

        String hashedTopic = hashing.md5Hash(topic);

        ArrayList<Broker> brokers = this.getBrokers();
        Broker selected = null;
        for (Broker broker : brokers) {
            // TODO: Cover all cases
            switch (broker.hash.compareTo(hashedTopic)) {
                case -1:
                case 0:
                    selected = broker;
                    break;
                case 1:
            }
        }
        return selected;
    }

    /**
     * Pushes data to the specified topic
     *
     * @param topic Topic to push data.
     * @param value Data to push.
     */
    public void push(String topic, Value value) {
    }

    /**
     * Notifies about a failed push operation.
     *
     * @param broker The Broker to notify.
     */
    public void notifyFailure(Broker broker) {
        Connection connection = super.connect(broker.config.getIp(), broker.config.getPort());

        try {
            connection.out.writeUTF("PushFailed");
            connection.out.writeUTF(channelName.channelName);

            connection.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    /**
     * Notifies the Broker about new content (hashtag)
     *
     * @param hashtag The hashtag to be notified.
     */
    public void notifyBrokersForHashTags(String hashtag, boolean add) {
        Broker broker = hashTopic(hashtag);
        Connection connection = super.connect(broker.config.getIp(), broker.config.getPort());

        try {
            if (add) {
                connection.out.writeUTF("AddHashTag");
            } else {
                connection.out.writeUTF("RemoveHashTag");
            }

            connection.out.writeUTF(channelName.channelName);
            connection.out.writeUTF(hashtag);
            connection.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    /**
     * Generate chunks of a Video file.
     * It extracts Video's metadata using the Apache Tika Library.
     *
     * @param filename The filename to open.
     * @return An ArrayList with all the chunks.
     */
    public ArrayList<Value> generateChunks(String filename) {
        return null;
    }

    /**
     * Opens a connections to the specified IP and port
     * and sends registration messages.
     *
     * @param ip   The IP to open the connection.
     * @param port The port to open the connection.
     * @return A Connection object.
     */
    @Override
    public Connection connect(String ip, int port) {
        Connection connection = super.connect(ip, port);

        /* Send messages to broker
         * Receive serialized Broker object
         */
        return connection;
    }

    @Override
    public void disconnect(Connection connection) {
        /* Send disconnection messages to broker
         * Call the super method to close the streams etc.
         * Remove it from the HashMap
         */
        super.disconnect(connection);
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(config.getPublisherPort());

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
        private Publisher publisher;

        public Handler(Socket socket, Publisher publisher) {
            this.socket = socket;
            this.publisher = publisher;
        }

        @Override
        public void run() {
            // Handle Broker requests
        }
    }
}
