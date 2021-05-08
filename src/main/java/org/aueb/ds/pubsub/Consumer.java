package org.aueb.ds.pubsub;

import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

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

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    public void playData(ArrayList<Value> video) {

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
                if (socket != null)
                    socket.close();
            } catch (IOException io) {
                System.out.println("Error: problem in input/output" + io.getMessage());
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
