package org.aueb.ds.pubsub.publisher;

import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.model.pubsub.Connection;
import org.aueb.ds.model.video.ChannelName;
import org.aueb.ds.model.video.Value;
import org.aueb.ds.pubsub.AppNode;
import org.aueb.ds.pubsub.Broker;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

public class Publisher extends AppNode implements Runnable, Serializable {

    public static final String TAG = "[Publisher] ";
    public AppNodeConfig config;

    private ChannelName channelName;
    private boolean acceptingConnections = true;
    private static final long serialVersionUID = -6645374596536043061L;

    public Publisher() {

    }

    public Publisher(String channelName) {
        this.channelName = new ChannelName(channelName);
    }

    @Override
    public boolean init() {
        return true;
    }

    /**
     * Adds video from the current publisher
     *
     * @param fileName the name of the .mp4 video file
     */
    public synchronized void addVideo(String fileName) {
    }

    /**
     * Removes a video from the the Publisher
     *
     * @param filename the video file name to be removed(either the video name or
     *                 the .mp4 file name)
     */
    public synchronized boolean removeVideo(String filename) {
    }

    /**
     * Add hashtag or channelName to ChannelName's List ("#viral"). Inform Brokers.
     *
     * @param topic HashTag or channelName added.
     */
    private synchronized void addHashTag(String topic) {
    }

    /**
     * Remove hashtag from ChannelName's List ("#viral"). Inform Brokers if hashtag
     * is not used in any other Publisher's video.
     *
     * @param hashtag HashTag removed.
     */
    private void removeHashTag(String hashtag) {
    }

    /**
     * Hashes the specified topic with MD5 algo.
     *
     * @param topic Topic to hash.
     * @return The broker which is responsible for the specified topic.
     */
    private Broker hashTopic(String topic) {
        return null;
    }

    /**
     * Pushes data to the specified topic
     *
     * @param connection The Connection object to send data on top of it.
     * @param videos     Videos to be send.
     */
    protected void push(Connection connection, HashSet<String> videos) throws IOException {
    }

    public ChannelName getChannelName() {
        return channelName;
    }

    /**
     * Notifies the Broker about new content (hashtag)
     *
     * @param hashtag The hashtag to be notified.
     */
    protected void notifyBrokersForHashTags(String hashtag, boolean add) {
    }

    /**
     * Generate chunks of a Video file. It extracts Video's metadata using the
     * Apache Tika Library.
     *
     * @param filename The filename to open.
     * @return An ArrayList with all the chunks.
     */
    private ArrayList<Value> generateChunks(String filename) {
        return null;
    }

    /**
     * Opens a connections to the specified IP and port and sends registration
     * messages.
     *
     * @param ip   The IP to open the connection.
     * @param port The port to open the connection.
     * @return A Connection object.
     */
    @Override
    public Connection connect(String ip, int port) {
        return null;
    }

    @Override
    public void disconnect(Connection connection) {
    }

    private void cleanup() {
        HashSet<Broker> registeredBrokers = new HashSet<>();

        synchronized (this) {
            // Find on which brokers we have been registered on
            channelName.hashtagsPublished.forEach(hashtag -> registeredBrokers.add(hashTopic(hashtag)));

            // Unregister topics
            channelName.hashtagsPublished.forEach(hashtag -> notifyBrokersForHashTags(hashtag, false));
        }

        // Unregister Publisher from all Brokers
        registeredBrokers.forEach(broker -> {
            try {
                Connection connection = super.connect(broker.config.getIp(), broker.config.getPort());
                disconnect(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Stop receiving new connections
        this.acceptingConnections = false;

        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("Thread-"))
                .forEach(thread -> {
                    if (thread.isAlive())
                        thread.interrupt();
                });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Publisher publisher = (Publisher) o;
        return channelName.channelName.equals(publisher.channelName.channelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName.channelName);
    }

    @Override
    public void run() {
        new Thread(this::init).start();

        // If the JVM is shutting down call cleanup()
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println(TAG + "Shutting down gracefully...");
            cleanup();
        }));

        try {
            ServerSocket serverSocket = new ServerSocket(config.getPublisherPort());
            while (acceptingConnections) {
                Socket socket = serverSocket.accept();
                Thread handler = new Thread(new Handler(socket, this));
                handler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
