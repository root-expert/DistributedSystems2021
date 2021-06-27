package org.aueb.ds.pubsub.consumer;

import org.aueb.ds.model.video.Value;
import org.aueb.ds.pubsub.Broker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

import static org.aueb.ds.pubsub.consumer.Consumer.TAG;

public class Handler implements Runnable {

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
            // Close streams
            out.flush();
            out.close();
            in.close();
            socket.close();
        } catch (IOException io) {
            System.out.println(TAG + "Error: problem in input/output" + io.getMessage());
            io.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
