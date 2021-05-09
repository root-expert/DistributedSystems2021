package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Consumer extends AppNode implements Runnable {

    protected String channelName;
    private HashMap<Broker, HashSet<String>> hashtagInfo = new HashMap<>();
    private static final String TAG = "[Consumer] ";

    public Consumer(AppNodeConfig conf) {
        super(conf);
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

                hashtagInfo.forEach((broker, strings) -> System.out.println(broker));

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
                System.out.println("The topic does not exist. Redirecting to correct brocker.");
                disconnect(connection);
                subscribe((Broker) connection.in.readObject(), topic);
            } else if (exitCode == -1) {
                System.out.println("The topic does not exist. Subscription failed.");
            } else if (exitCode == -2) {
                System.out.println("Subscription successfull. There are no videos currently to receive.");
            } else {
                System.out.println("Subscription successfull. Receiving videos for new topic.");
                // Receive the number of videos to be viewed
                int numVideos = connection.in.readInt();
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
                System.out.println("Successful unsubscription from topic.");
            } else if (exitCode == -1) {
                System.out.println("The topic to be unsubscribed from does not exist.");
            } else {
                System.out.println("Unsubsciption Failed. The consumer was not subscribed to the topic, in the first place.");
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
        Collections.sort(video);

        try {
            File file = new File(System.getProperty("user.dir") + "/out/" + video.get(0).videoFile.videoName + String.join("", video.get(0).videoFile.associatedHashtags) + ".mp4");
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

    @Override
    public void run() {
        init();

        new Thread(() -> {
            while (true) {
                System.out.println("Please enter a topic to subscribe: ");
                Scanner scanner = new Scanner(System.in);

                String topic = scanner.next();

                Broker selected = null;

                for (Broker broker : hashtagInfo.keySet()) {
                    if (hashtagInfo.get(broker).contains(topic)) {
                        selected = broker;
                        break;
                    }
                }

                // TODO : Change me
                if (selected == null)
                    System.out.println("Couldn't find broker");
                else
                    subscribe(selected, topic);
                try {
                    Thread.sleep(10000);
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
                        consumer.hashtagInfo.put(broker, hashtags);
                    } else if (action.equals("end")) {
                        break;
                    }
                }
                // Close streams if defined
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
