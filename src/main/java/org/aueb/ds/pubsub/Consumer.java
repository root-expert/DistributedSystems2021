package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Consumer extends AppNode implements Runnable, Serializable {

    private static final long serialVersionUID = -8644673594536043061L;

    protected String channelName;
    private HashMap<Broker, HashSet<String>> hashtagInfo = new HashMap<>();
    private ArrayList<String> subscribedItems = new ArrayList<>(); // Contains Consumer's subscribed topics
                                                                   // (channelName/Hashtags)
    public static final String TAG = "[Consumer] ";

    protected AppNodeConfig config;

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

        // Make directory to save files
        new File(System.getProperty("user.dir") + "/out/").mkdirs();

        channelName = config.getChannelName();
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

    /**
     * Subscribe the consumer to the specified topic.
     *
     * @param broker The broker to subscribe.
     * @param topic  The topic to be subscribed on.
     */
    public void subscribe(Broker broker, String topic) {
        Connection connection = null;

        try {
            connection = connect(broker.config.getIp(), broker.config.getPort());
            connection.out.writeUTF("subscribe");
            connection.out.writeObject(this);
            connection.out.writeUTF(topic);
            connection.out.flush();
            int exitCode = connection.in.readInt();

            if (exitCode == 1) {
                System.out.println(TAG + "The topic does not exist. Redirecting to correct broker.");

                subscribe((Broker) connection.in.readObject(), topic);
            } else if (exitCode == -1) {
                System.out.println(TAG + "The topic does not exist. Please pick a different one!");
            } else if (exitCode == -2) {
                System.out.println(TAG + "Subscription successful. There are no videos currently to receive!");
            } else {
                System.out.println(TAG + "Subscription successful. Receiving videos for new topic!");
                // Receive the number of videos to be viewed
                int numVideos = connection.in.readInt();
                if (numVideos == 0)
                    System.out.println(TAG + "No videos available right now. Try again later!");

                for (int video = 0; video < numVideos; video++) {
                    // Construct the video
                    ArrayList<Value> fullVideo = new ArrayList<>();
                    int numChunks = connection.in.readInt();
                    for (int bin = 0; bin < numChunks; bin++) {
                        fullVideo.add((Value) connection.in.readObject());
                    }
                    // And store it locally
                    playData(fullVideo);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException cf) {
            System.out.println("Error: invalid cast :" + cf.getMessage());
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
        return Objects.hash(hashtagInfo);
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

    @Override
    public void run() {
        init();

        new Thread(() -> {
            while (true) {
                System.out.println("[1] Subscribe");
                System.out.println("[2] Unsubscribe");
                System.out.println("[3] Exit");

                Scanner scanner = new Scanner(System.in);
                int ans;
                do {
                    System.out.print("Choose a number for your action: ");
                    ans = scanner.nextInt();
                } while (ans != 1 && ans != 2 && ans != 3);

                if (ans == 1) {
                    System.out.println(TAG + "Please enter a topic to subscribe: ");
                    String topic = scanner.next();

                    if (subscribedItems.contains(topic)) {
                        System.out.println("You are already subscribed to " + topic);
                    } else {
                        subscribe(findBroker(topic), topic);
                        subscribedItems.add(topic);
                    }
                } else if (ans == 2) {
                    System.out.println(TAG + "Please enter a topic to unsubscribe: ");
                    String topic = scanner.next();

                    if (!subscribedItems.contains(topic)) {
                        System.out.println("You are not subscribed to " + topic);
                    } else {
                        unsubscribe(findBroker(topic), topic);
                        subscribedItems.remove(topic);
                    }
                } else {
                    System.out.println("Exiting..");
                    break;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        try {
            ServerSocket serverSocket = new ServerSocket(config.getConsumerPort());

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

                while (!socket.isClosed()) {
                    // Reading the action required
                    String action = in.readUTF();
                    if (action.equals("updateBrokerList")) {
                        Broker broker = (Broker) in.readObject();
                        HashSet<String> hashtags = (HashSet<String>) in.readObject();

                        synchronized (consumer) {
                            consumer.hashtagInfo.put(broker, hashtags);
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
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
