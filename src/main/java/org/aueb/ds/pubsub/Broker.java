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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class Broker implements Node, Serializable, Runnable, Comparable<Broker> {

    private static final long serialVersionUID = -6648374896546053061L;

    private HashSet<Consumer> registeredUsers = new HashSet<>();
    private HashSet<Publisher> registeredPublishers = new HashSet<>();

    // Contains which hashtags each Publisher is responsible for
    private HashMap<Publisher, HashSet<String>> publisherHashtags = new HashMap<>();

    /*
     * videoList: is a list with all videos(already chunked from the publisher)
     * concerning a certain topic. It only contains the topics the broker is
     * associated with.
     */
    private HashMap<String, ArrayList<ArrayList<Value>>> videoList = new HashMap<>();

    // userHashtags: a hashmap that holds the set of Consumer objects that are associated with this topic.
    private HashMap<String, HashSet<Consumer>> userHashtags = new HashMap<>();

    // brokerAssociatedHashtags: a hashmap that contains all the hashtag each broker is associated with.
    private HashMap<Broker, HashSet<String>> brokerAssociatedHashtags = new HashMap<>();
    private static final String TAG = "[Broker] ";

    protected BrokerConfig config;
    protected String hash;

    public Broker() {
    }

    public Broker(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public void init() {
        this.hash = new Hashing().md5Hash(config.getIp() + config.getPort());

        brokerAssociatedHashtags.put(this, new HashSet<>());
    }

    public void notifyPublisher(String topic) {
        // if at least one publisher is related to the topic
        Connection connection = null;
        /*
         * For every publisher that is affiliated with the broker, either if the channel
         * name matches the topic or is a hashtag that the Publisher has content of
         */
        for (Publisher pu : registeredPublishers) {
            if (publisherHashtags.get(pu).contains(topic) || pu.getChannelName().channelName.equals(topic)) {
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
                    System.out.println(TAG + "Error: there was problem in input/output: " + io.getMessage());
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

            Connection connection = null;

            try {
                connection = connect(broker.config.getIp(), broker.config.getPort());
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

        Connection connection = null;
        try {
            connection = connect(publisher.config.getIp(), publisher.config.getPublisherPort());
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

                    ArrayList<ArrayList<Value>> videos = videoList.get(topic);

                    if (videos != null)
                        videos.add(chunkList);
                    else {
                        videoList.put(topic, new ArrayList<>());
                        videoList.get(topic).add(chunkList);
                    }
                }
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            disconnect(connection);
        }
    }

    @Override
    public ArrayList<Broker> getBrokers() {
        return null;
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
        if (connection == null)
            return;

        try {
            connection.out.writeUTF("end");
            connection.out.flush();

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
    /*
     * Update Consumer's Broker list after a hashtag has been added or removed.
     */
    public void updateNodes() {
        for (Consumer consumer : registeredUsers) {
            Connection connection = null;
            try {
                connection = connect(consumer.config.getIp(), consumer.config.getPublisherPort());
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
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

        Runnable brokerConnection = () -> {
            for (String address : this.config.getBrokers()) {
                boolean connected = false;

                while (!connected) {
                    String ip = address.split(":")[0];
                    int port = Integer.parseInt(address.split(":")[1]);

                    Connection connection = null;
                    try {
                        connection = connect(ip, port);
                        connection.out.writeUTF("addBroker");
                        connection.out.writeObject(this);
                        connection.out.flush();

                        connected = true;
                    } catch (IOException e) {
                        System.out.println(
                                TAG + "Broker with address " + address + " seems down. Trying again in 5 seconds");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    } finally {
                        disconnect(connection);
                    }
                }
            }
        };

        new Thread(brokerConnection).start();

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
                        if (!broker.registeredPublishers.contains(pu)) {
                            broker.registeredPublishers.add(pu);
                            broker.publisherHashtags.put(pu, new HashSet<>());
                        }
                    } else if (action.equals("disconnectP")) {
                        // Receive the channel to remove from the registered publishers
                        String cn = in.readUTF();

                        Publisher toBeRemoved = broker.registeredPublishers
                                .stream().filter(it -> it.getChannelName().channelName.equals(cn))
                                .findFirst().orElse(null);

                        if (toBeRemoved != null)
                            broker.registeredPublishers.remove(toBeRemoved);
                        else
                            throw new IllegalStateException("There doesn't exist a publisher with that channel name");
                    } else if (action.equals("getBrokerInfo")) {
                        out.writeObject(broker.brokerAssociatedHashtags);
                        out.flush();
                    } else if (action.equals("AddHashTag")) {
                        // Receive the topic to add into the broker (if it doesn't already exist)
                        String topic = in.readUTF();
                        String channelName = in.readUTF();
                        // if it does not already exist in this broker's collection add it
                        broker.brokerAssociatedHashtags.get(broker).add(topic);

                        // Update the hashtags of the specified publisher
                        broker.publisherHashtags.keySet().stream()
                                .filter(it -> it.getChannelName().channelName.equals(channelName))
                                .findFirst()
                                .ifPresent(publisher -> broker.publisherHashtags.get(publisher).add(topic));

                        broker.videoList.put(topic, new ArrayList<>());
                        broker.notifyBrokersOnChanges();
                        // Update consumer's broker list
                        broker.updateNodes();
                    } else if (action.equals("RemoveHashTag")) {
                        // Receive the topic to remove from the broker (if it exists)
                        String topic = in.readUTF();
                        int count = 0;
                        for (Publisher pu : broker.registeredPublishers) {
                            if (pu.getChannelName().hashtagsPublished.contains(topic))
                                count++;
                        }
                        if (count == 1) {
                            broker.videoList.remove(topic);
                            broker.brokerAssociatedHashtags.get(broker).remove(topic);
                            broker.notifyBrokersOnChanges();
                            // Update consumer's broker list
                            broker.updateNodes();
                        }
                    } else if (action.equals("subscribe")) {
                        // Read the consumer object and the topic
                        Consumer subscriber = (Consumer) in.readObject();
                        String topic = in.readUTF();
                        // If the broker is associated with this topic
                        // and therefore provide videos for it
                        if (broker.brokerAssociatedHashtags.get(broker).contains(topic)) {
                            // If there are no consumers subscribed to this topic
                            // initialise the repository of consumers that will be
                            // informed with the video
                            if (!broker.userHashtags.containsKey(topic)) {
                                broker.userHashtags.put(topic, new HashSet<>());
                            }
                            // Add the Consumer to the repository and reconstruct the list
                            // of the videos to be sent to them
                            broker.userHashtags.get(topic).add(subscriber);
                            ArrayList<ArrayList<Value>> videos = broker.videoList.get(topic);
                            if (videos != null)
                                broker.videoList.get(topic).clear();
                            broker.notifyPublisher(topic);

                            if (!broker.videoList.get(topic).isEmpty()) {
                                out.writeInt(0);
                                out.flush();
                                // Create a Set with the videos to be sent the consumer, where
                                // the consumer's own videos are excluded
                                HashSet<ArrayList<Value>> toSend = new HashSet<>();
                                for (ArrayList<Value> video : broker.videoList.get(topic)) {
                                    if (!video.get(0).videoFile.channelName.equals(subscriber.channelName))
                                        toSend.add(video);
                                }
                                // Send the amount of videos to be sent
                                int numVideos = toSend.size();
                                out.writeInt(numVideos);
                                out.flush();
                                for (ArrayList<Value> video : toSend) {
                                    // Send the amount chunks each video has
                                    out.writeInt(video.size());
                                    out.flush();
                                    for (Value chunk : video) {
                                        out.writeObject(chunk);
                                        out.flush();
                                    }
                                }
                            } else {
                                out.write(-2);
                                out.flush();
                            }
                        } else {
                            boolean found = false;
                            for (Broker brock : broker.brokerAssociatedHashtags.keySet()) {
                                if (broker.brokerAssociatedHashtags.get(brock).contains(topic)) {
                                    found = true;
                                    out.writeInt(1);
                                    out.writeObject(brock);
                                    break;
                                }
                            }
                            if (!found) {
                                out.writeInt(-1);
                            }
                            out.flush();
                        }
                    } else if (action.equals("unsubscribe")) {
                        // Receive the Consumer object and the topic for unsubscription
                        Consumer subscriber = (Consumer) in.readObject();
                        String topic = in.readUTF();

                        // Check if the broker has the topic and is actively handling it
                        if (broker.brokerAssociatedHashtags.get(broker).contains(topic)) {
                            // The subscriber is removed from the subscription hashset
                            boolean completed = broker.userHashtags.get(topic).remove(subscriber);
                            // If the removal was successful inform with an exit code
                            if (completed) {
                                out.writeInt(0);
                            } else {
                                out.writeInt(-2);
                            }
                        } else {
                            out.writeInt(-1);
                        }
                        out.flush();
                    } else if (action.equals("register")) {
                        Consumer consumer = (Consumer) in.readObject();
                        broker.registeredUsers.add(consumer);
                    } else if (action.equals("unregister")) {
                        Consumer consumer = (Consumer) in.readObject();
                        broker.registeredUsers.remove(consumer);
                    } else if (action.equals("addBroker")) {
                        Broker newBroker = (Broker) in.readObject();
                        broker.brokerAssociatedHashtags.put(newBroker, new HashSet<>());
                        System.out.println(TAG + "New broker added! Port = " + newBroker.config.getPort());
                    } else if (action.equals("removeBroker")) {
                        Broker toBeRemoved = (Broker) in.readObject();
                        broker.brokerAssociatedHashtags.remove(toBeRemoved);
                        System.out.println(TAG + "Broker removed! Port = " + toBeRemoved.config.getPort());
                    } else if (action.equals("notifyNewHashtags")) {
                        String hash = in.readUTF();
                        HashSet<String> hashtags = (HashSet<String>) in.readObject();

                        for (Broker toBeUpdated : broker.brokerAssociatedHashtags.keySet()) {
                            if (toBeUpdated.hash.equals(hash)) {
                                broker.brokerAssociatedHashtags.put(toBeUpdated, hashtags);
                                break;
                            }
                        }
                        broker.updateNodes();
                    } else if (action.equals("end")) {
                        break;
                    }
                }
                // Close streams if defined
                out.flush();
                out.close();
                in.close();
                socket.close();
            } catch (ClassNotFoundException cf) {
                System.out.println(TAG + "Error: invalid cast" + cf.getMessage());
            } catch (NullPointerException nu) {
                System.out.println(TAG + "Error: Inappropriate connection object, connection failed " + nu.getMessage());
                nu.printStackTrace();
            } catch (IOException io) {
                System.out.println(TAG + "Error: problem in input/output " + io.getMessage());
                io.printStackTrace();
            } catch (IllegalStateException e) {
                System.out.println(TAG + "Error: " + e.getMessage());
            }
        }
    }
}
