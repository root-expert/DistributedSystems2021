package org.aueb.ds.pubsub.consumer;

import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.model.pubsub.Connection;
import org.aueb.ds.model.video.Value;
import org.aueb.ds.pubsub.AppNode;
import org.aueb.ds.pubsub.Broker;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class Consumer extends AppNode implements Runnable, Serializable {

    public static final String TAG = "[Consumer] ";
    public AppNodeConfig config;
    public String channelName;

    protected String cachePath;

    protected final HashMap<Broker, HashSet<String>> hashtagInfo = new HashMap<>();
    // Contains Consumer's subscribed topics
    protected final ArrayList<String> subscribedItems = new ArrayList<>();
    private boolean acceptingConnections = true;
    private static final long serialVersionUID = -8644673594536043061L;

    public Consumer() {
    }

    public Consumer(String cachePath) {
        this.cachePath = cachePath;
    }

    /**
     * Initializes Consumer's state. Connects to the first Broker retrieving the
     * rest of the Brokers.
     */
    @Override
    public boolean init() {
        return true;
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
        return false;
    }

    /**
     * Unsubscribe the consumer from the specified topic.
     *
     * @param broker The broker to which is currently subscribed.
     * @param topic  The topic to unsubscribe from.
     */
    public void unsubscribe(Broker broker, String topic) {
    }

    /**
     * Saves video file locally.
     *
     * @param video The Value object with video chunks.
     */
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
        return null;
    }

    /**
     * Unregisters the Consumer from the specified Broker.
     *
     * @param connection The Connection object to communicate with the broker
     */
    @Override
    public void disconnect(Connection connection) {
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
        return null;
    }

    private void cleanup() {

    }

    @Override
    public void run() {

    }
}
