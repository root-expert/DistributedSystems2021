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
        Connection connection = connect(config.getBrokerIP(), config.getBrokerPort());

        try {
            connection.in = new ObjectInputStream(connection.socket.getInputStream());
            connection.out = new ObjectOutputStream(connection.socket.getOutputStream());

            connection.out.writeUTF("getBrokerInfo");
            connection.out.flush();

            hashtagInfo.putAll((HashMap<Broker, HashSet<String>>) connection.in.readObject());
            System.out.println("Received broker's list.");

            ArrayList<Broker> brokerList = new ArrayList<>(hashtagInfo.keySet());
            this.setBrokers(brokerList);
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
        Connection connection = connect(broker.config.getIp(), broker.config.getPort());

        try {
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
    public Connection connect(String ip, int port) {
        // Open Socket connection with the Broker
        Connection connection = null;

        try {
            connection = super.connect(ip, port);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
