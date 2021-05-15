package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Consumer extends AppNode implements Runnable, Serializable {

    public static final String TAG = "[Consumer] ";

    protected AppNodeConfig config;
    protected String channelName;

    private HashMap<Broker, HashSet<String>> hashtagInfo = new HashMap<>();
    // Contains Consumer's subscribed topics
    private ArrayList<String> subscribedItems = new ArrayList<>();
    private boolean acceptingConnections = true;
    private static final long serialVersionUID = -8644673594536043061L;

    public Consumer() {
    }

    public Consumer(AppNodeConfig conf) {
        this.config = conf;
    }

    /**
     * Initializes Consumer's state. Connects to the first Broker retrieving the
     * rest of the Brokers.
     */
    @Override
    public void init() {
        channelName = config.getChannelName();

        // Make directory to save files
        new File(System.getProperty("user.dir") + "/out/").mkdirs();

        Connection connection = null;
        boolean connected = false;

        while (!connected) {
            try {
                connection = super.connect(config.getBrokerIP(), config.getBrokerPort());
                connection.out.writeUTF("getBrokerInfo");
                connection.out.flush();

                connected = true;
                System.out.println(TAG + "Acquired first connection to broker.");

                hashtagInfo.putAll((HashMap<Broker, HashSet<String>>) connection.in.readObject());
                System.out.println(TAG + "Received broker's list.");

                hashtagInfo.forEach((broker, strings) -> System.out
                        .println("Broker: " + broker.config.getPort() + " tags: " + strings));

                ArrayList<Broker> brokerList = new ArrayList<>(hashtagInfo.keySet());
                this.setBrokers(brokerList);
            } catch (IOException | ClassNotFoundException e) {
                System.out.println(TAG + "Broker seems down. Trying again in 5 seconds");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            } finally {
                super.disconnect(connection);
            }
        }
    }

    public ArrayList<String> getSubscribedItems() {
        return subscribedItems;
    }

    /**
     * Subscribe the consumer to the specified topic.
     *
     * @param broker The broker to subscribe.
     * @param topic  The topic to be subscribed on.
     * @return true if the subscription was successful else false
     */
    public boolean subscribe(Broker broker, String topic) {
        Connection connection = null;

        if (subscribedItems.contains(topic)) {
            System.out.println(TAG + "You are already subscribed to " + topic);
            return false;
        }

        try {
            connection = connect(broker.config.getIp(), broker.config.getPort());
            connection.out.writeUTF("subscribe");
            connection.out.writeObject(this);
            connection.out.writeUTF(topic);
            connection.out.flush();
            int exitCode = connection.in.readInt();

            if (exitCode == 1) {
                System.out.println(TAG + "The topic does not exist. Redirecting to correct broker.");

                return subscribe((Broker) connection.in.readObject(), topic);
            } else if (exitCode == -1) {
                System.out.println(TAG + "The specified topic does not exist.");
                return false;
            } else {
                System.out.println(TAG + "Subscription successful!");
                subscribedItems.add(topic);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException cf) {
            System.out.println("Error: invalid cast :" + cf.getMessage());
            return false;
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
        if (!subscribedItems.contains(topic)) {
            System.out.println(TAG + "You are not subscribed to " + topic);
            return;
        }

        Connection connection = null;

        try {
            connection = super.connect(broker.config.getIp(), broker.config.getPort());
            connection.out.writeUTF("unsubscribe");
            connection.out.writeObject(this);
            connection.out.writeUTF(topic);
            connection.out.flush();
            int exitCode = connection.in.readInt();
            if (exitCode == 0) {
                System.out.println(TAG + "Successful unsubscription from topic.");
                subscribedItems.remove(topic);
            } else if (exitCode == -1) {
                System.out.println(TAG + "The topic to be unsubscribed from does not exist.");
            } else {
                System.out.println(TAG
                        + "Unsubsciption Failed. The consumer was not subscribed to the topic, in the first place.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    /**
     * Saves video file locally.
     *
     * @param video The Value object with video chunks.
     */
    public void playData(ArrayList<Value> video) {

        String videoName = video.get(0).videoFile.videoName.split("_")[0];

        try {
            File file = new File(System.getProperty("user.dir") + "/out/" + videoName
                    + String.join("", video.get(0).videoFile.associatedHashtags) + ".mp4");
            FileOutputStream fos = new FileOutputStream(file);
            for (Value v : video) {
                fos.write(v.videoFile.videoFileChunk);
            }
            fos.flush();
            fos.close();
        } catch (FileNotFoundException f) {
            System.out.println("Error: could not find file: " + f.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(TAG + "Successfully recompiled " + videoName);
    }

    /**
     * Registers a Consumer to the specified Broker.
     *
     * @param ip   The IP to open the connection to.
     * @param port The port to open the connection to.
     * @return A Connection object.
     */
    @Override
    public Connection connect(String ip, int port) throws IOException {
        // Open Socket connection with the Broker
        Connection connection = null;

        connection = super.connect(ip, port);
        connection.out.writeUTF("register");
        connection.out.writeObject(this);
        connection.out.flush();

        return connection;
    }

    /**
     * Unregisters the Consumer from the specified Broker.
     *
     * @param connection The Connection object to communicate with the broker
     */
    @Override
    public void disconnect(Connection connection) {
        if (connection == null) return;

        try {
            connection.out.writeUTF("unregister");
            connection.out.writeObject(this);
            connection.out.flush();
        } catch (IOException e) {
            System.out.println(TAG + "Error while unregistering");
            e.printStackTrace();
        }

        super.disconnect(connection);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Consumer consumer = (Consumer) o;
        return this.channelName.equals(consumer.channelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName);
    }

    /**
     * Finds the Broker that is responsible for the topic or a random one.
     *
     * @param topic ChannelName or Hashtag to search for
     * @return selected Broker
     */
    public Broker findBroker(String topic) {
        Broker selected = null;

        synchronized (this) {
            for (Broker broker : hashtagInfo.keySet()) {
                if (hashtagInfo.get(broker).contains(topic)) {
                    selected = broker;
                }
            }

            if (selected == null) {
                System.out.println(TAG + "Couldn't find broker. Picking a random one");
                int randomIdx = new Random().nextInt(hashtagInfo.size());
                selected = (Broker) hashtagInfo.keySet().toArray()[randomIdx];
            }
        }
        return selected;
    }

    private void cleanup() {
        // Find which broker is responsible for every subscribed topic
        HashSet<Broker> registeredBrokers = new HashSet<>();

        synchronized (this) {
            subscribedItems.forEach(topic -> registeredBrokers.add(findBroker(topic)));

            // Unsubscribe from every topic
            subscribedItems.forEach(topic -> unsubscribe(findBroker(topic), topic));
        }

        // Unregister from every broker
        registeredBrokers.forEach(broker -> {
            Connection connection = null;
            try {
                connection = super.connect(broker.config.getIp(), broker.config.getPort());
            } catch (IOException e) {
                e.printStackTrace();
            }

            disconnect(connection);
        });

        // Stop receiving connections
        this.acceptingConnections = false;

        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("Thread-"))
                .forEach(thread -> {
                    if (thread.isAlive())
                        thread.interrupt();
                });
    }

    @Override
    public void run() {
        init();

        // If the JVM is shutting down call cleanup()
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println(TAG + "Shutting down gracefully...");
            cleanup();
        }));

        try {
            ServerSocket serverSocket = new ServerSocket(config.getConsumerPort());

            while (acceptingConnections) {
                Socket socket = serverSocket.accept();
                Thread handler = new Thread(new Handler(socket, this));
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class Handler implements Runnable {
        private final Socket socket;
        private final Consumer consumer;

        public Handler(Socket socket, Consumer consumer) {
            this.socket = socket;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                // Initializing output and input streams
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                while (!socket.isClosed() && !Thread.interrupted()) {
                    // Reading the action required
                    String action = in.readUTF();
                    if (action.equals("updateBrokerList")) {
                        Broker broker = (Broker) in.readObject();
                        HashSet<String> hashtags = (HashSet<String>) in.readObject();

                        synchronized (consumer) {
                            consumer.hashtagInfo.put(broker, hashtags);
                        }
                    } else if (action.equals("newVideos")) {
                        System.out.println();
                        System.out.println(TAG + "Receiving new videos...");
                        int numOfVideos = in.readInt();

                        for (int i = 0; i < numOfVideos; i++) {
                            ArrayList<Value> video = new ArrayList<>();
                            int numOfChunks = in.readInt();
                            for (int j = 0; j < numOfChunks; j++) {
                                video.add((Value) in.readObject());
                            }

                            consumer.playData(video);
                        }
                    } else if (action.equals("forceUnsubscribe")) {
                        String topic = in.readUTF();

                        synchronized (consumer) {
                            consumer.subscribedItems.remove(topic);
                            System.out.println();
                            System.out.println(TAG + "You were unsubscribed from " + topic + " as it is no longer available.");
                        }
                    } else if (action.equals("end")) {
                        break;
                    }
                }
                // Close streams if defined
                out.flush();
                out.close();
                in.close();
                socket.close();
            } catch (IOException io) {
                System.out.println("Error: problem in input/output" + io.getMessage());
                io.printStackTrace();
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
