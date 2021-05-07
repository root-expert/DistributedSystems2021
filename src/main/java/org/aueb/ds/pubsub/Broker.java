package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Node;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.BrokerConfig;
import org.aueb.ds.util.Hashing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Broker implements Node, Serializable, Runnable, Comparable<Broker> {

    private ArrayList<Consumer> registeredUsers = new ArrayList<>();
    private ArrayList<Publisher> registeredPublishers = new ArrayList<>();
    private HashMap<String, ArrayList<ArrayList<Value>>> videoList = new HashMap<>();
    private HashMap<String,HashSet<Consumer>> userHashtags=new HashMap<>();
    private HashMap<Broker, HashSet<String>> brokerAssociatedHashtags = new HashMap<>();

    protected BrokerConfig config;
    protected String hash;

    public Broker(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public void init() {
        this.hash = new Hashing().md5Hash(config.getIp() + config.getPort());

        brokerAssociatedHashtags.put(this, new HashSet<>());
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
        for (Publisher pu : registeredPublishers) {
            if (pu.getChannelName().hashtagsPublished.contains(topic)
                    || pu.getChannelName().channelName.equals(topic)) {
                try {
                    connection = this.connect(pu.config.getIp(), pu.config.getPublisherPort());
                    connection.out.writeUTF("notify");
                    connection.out.writeUTF(topic);
                    connection.out.flush();
                    int exitCode = connection.in.readInt();
                    if (exitCode == 0) {
                        pull(pu, topic);
                    }
                    disconnect(connection);
                } catch (IOException io) {
                    System.out.println("Error: there was problem in input/output: " + io.getMessage());
                }
            }
        }
    }

    /**
     * Notifies the rest of the brokers for changes on the hashtags this specific
     * broker is responsible for.
     */
    public void notifyBrokersOnChanges() {
        for (Broker broker : brokerAssociatedHashtags.keySet()) {
            if (broker.hash.equals(this.hash))
                continue;

            Connection connection = connect(broker.config.getIp(), broker.config.getPort());

            try {
                connection.out.writeUTF("notifyNewHashtags");
                connection.out.writeUTF(hash);
                connection.out.writeObject(brokerAssociatedHashtags.get(this));
                connection.out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                disconnect(connection);
            }
        }
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
                    videoList.get(topic).add(chunkList);
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
            if (connection.in != null)
                connection.in.close();
            if (connection.out != null)
                connection.out.close();
            if (connection.socket != null)
                connection.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    /**
     * Update Consumer's Broker list after
     * a hashtag has been added or removed.
     */
    public void updateNodes() {
        for (Consumer consumer : registeredUsers) {
            Connection connection = connect(consumer.config.getIp(), consumer.config.getPublisherPort());
            try {
                connection.out.writeUTF("updateBrokerList");
                connection.out.writeObject(this);
                connection.out.writeObject(brokerAssociatedHashtags.get(this));
                connection.out.flush();

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                disconnect(connection);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)return true;
        if (o == null || getClass() != o.getClass())return false;
        Broker broker = (Broker) o;
        return hash.equals(broker.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash);
    }

    @Override
    public void run() {
        // Run initialization before accepting requests
        init();

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

    @Override
    public int compareTo(Broker broker) {
        return this.hash.compareTo(broker.hash);
    }

    private static class Handler implements Runnable {
        private final Socket socket;
        private final Broker broker;

        public Handler(Socket socket, Broker broker) {
            this.socket = socket;
            this.broker = broker;
        }

        @Override
        public void run() {
            try {
                // Initializing output and input streams
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                while (!socket.isClosed()) {
                    // Reading the action required
                    String action = in.readUTF();
                    if (action.equals("connectP")) {
                        // Receive the object that wants to be connected
                        Publisher pu = (Publisher) in.readObject();

                        // Add Publishers to the registered publishers
                        if (!broker.registeredPublishers.contains(pu)) broker.registeredPublishers.add(pu);
                    } else if (action.equals("disconnectP")) {
                        // Receive the channel to remove from the registered publishers
                        String cn = in.readUTF();
                        Publisher toBeRemoved = null;

                        // Search for the correct publisher
                        for (Publisher pu : broker.registeredPublishers) {
                            if (pu.getChannelName().channelName.equals(cn)) {
                                toBeRemoved = pu;
                            }
                        }

                        // If a channel to be removed has been found removes it
                        if (toBeRemoved != null) {
                            broker.registeredPublishers.remove(toBeRemoved);

                        } else {
                            throw new Exception("There doesn't exist a publisher with that channel name");
                        }
                    } else if (action.equals("getBrokerInfo")) {
                        out.writeObject(broker.brokerAssociatedHashtags);
                        out.flush();
                    } else if (action.equals("AddHashTag")) {
                        // Receive the topic to add into the broker (if it doesn't already exist)
                        String topic = in.readUTF();
                        // if it does not already exist in this broker's collection add it
                        broker.brokerAssociatedHashtags.get(broker).add(topic);
                        broker.videoList.put(topic, new ArrayList<>());
                        broker.notifyBrokersOnChanges();
                        // Update consumer's broker list
                        broker.updateNodes();

                    } else if (action.equals("RemoveHashTag")) {
                        // Receive the topic to remove from the broker (if it exists)
                        String topic = in.readUTF();
                        int count=0;
                        for (Publisher pu:broker.registeredPublishers){
                            if (pu.getChannelName().hashtagsPublished.contains(topic)) count++;
                        }
                        if(count==1){
                            broker.videoList.remove(topic);
                            broker.brokerAssociatedHashtags.get(broker).remove(topic);
                            broker.notifyBrokersOnChanges();
                            // Update consumer's broker list
                            broker.updateNodes();
                        }
                    }else if(action.equals("subscribe")){
                        try {
                            Consumer subscriber=(Consumer)in.readObject();
                            String topic=in.readUTF();
                            if (broker.brokerAssociatedHashtags.get(broker).contains(topic)){
                                // TODO: discuss and implement what happens when a consumer
                                // TODO: subscribes to a broker they are not registered to
                                if(!broker.userHashtags.containsKey(topic)){
                                    broker.userHashtags.put(topic,new HashSet<>());
                                }
                                broker.userHashtags.get(topic).add(subscriber);
                                broker.videoList.clear();
                                broker.notifyPublisher(topic);
                                int numberOfVideos=broker.videoList.get(topic).size();
                                for (Consumer sub:(Consumer[])broker.userHashtags.get(topic).toArray()){
                                    Connection socket=broker.connect(sub.config.getIp(), sub.config.getConsumerPort());
                                    broker.disconnect(socket);
                                }
                            }
                        } catch (ClassCastException cc) {
                            System.out.println("Error: ");
                        }
                    }
                }
                // Close streams if defined
                out.close();
                in.close();
                if (socket != null)
                    socket.close();
            } catch (ClassNotFoundException cf) {
                System.out.println("Error: invalid cast" + cf.getMessage());
            } catch (NullPointerException nu) {
                System.out.println("Error: Inappropriate connection object, connection failed" + nu.getMessage());
            } catch (IOException io) {
                System.out.println("Error: problem in input/output" + io.getMessage());
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
