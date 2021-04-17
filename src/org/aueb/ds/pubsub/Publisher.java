package org.aueb.ds.pubsub;

import org.aueb.ds.model.Node;
import org.aueb.ds.model.Value;

import java.util.ArrayList;

public class Publisher extends Node {

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
        return null;
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
}
