package org.aueb.ds.pubsub;

import org.aueb.ds.model.ChannelName;
import org.aueb.ds.model.Value;
import org.aueb.ds.util.Hashing;

import java.net.Socket;
import java.util.ArrayList;

public class Publisher extends AppNode implements Runnable {

    private ChannelName channelName;
    private ArrayList<Broker> brokers = new ArrayList<>();

    public void addHashTag(String hashtag) {

    }

    public void removeHashTag(String hashtag) {

    }

    public void getBrokerList() {

    }

    /**
     * Hashes the specified topic with MD5 algo.
     * @param topic Topic to hash.
     * @return The broker which is responsible for the specified topic.
     */
    public Broker hashTopic(String topic) {
        Hashing hashing = new Hashing();

        String hashedTopic = hashing.md5Hash(topic);

        Broker selected = null;
        for (Broker broker : brokers) {
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
     * @param topic Topic to push data.
     * @param value Data to push.
     */
    public void push(String topic, Value value) {
    }

    /**
     * Notifies about a failed push operation.
     * @param broker The Broker to notify.
     */
    public void notifyFailure(Broker broker) {

    }

    /**
     * Notifies the Broker about new content (hashtag)
     * @param hashtag The hashtag to be notified.
     */
    public void notifyBrokersForHashTags(String hashtag) {

    }

    public ArrayList<Value> generateChunks(String i) {
        return null;
    }

    @Override
    public Socket connect(String ip, int port) {
        Socket socket = super.connect(ip, port);

        // send messages to broker
        return null;
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    @Override
    public void run() {

    }
}
