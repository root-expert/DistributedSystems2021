package org.aueb.ds.pubsub;

import org.aueb.ds.model.Node;
import org.aueb.ds.model.Value;
import org.aueb.ds.util.Hashing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class Publisher implements Node {
    private ArrayList<Broker> brokers = new ArrayList<>();
    private Socket socket;
    ObjectInputStream in;
    ObjectOutputStream out;

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
    public ArrayList<Broker> getBrokers() {
        ArrayList<Broker> receivedBrokers = null;
        try {
            out.writeUTF("getBrokerList");
            out.flush();

            receivedBrokers = (ArrayList<Broker>) in.readObject();
            System.out.println("Received broker list!");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return receivedBrokers;
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }
}
