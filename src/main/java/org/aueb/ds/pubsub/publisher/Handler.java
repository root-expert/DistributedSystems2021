package org.aueb.ds.pubsub.publisher;

import org.aueb.ds.model.pubsub.Connection;
import org.aueb.ds.model.video.ChannelName;
import org.aueb.ds.model.video.Value;
import org.aueb.ds.pubsub.Broker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashSet;

import static org.aueb.ds.pubsub.publisher.Publisher.TAG;

public class Handler implements Runnable {

    private final Socket socket;
    private final Publisher publisher;

    public Handler(Socket socket, Publisher publisher) {
        this.socket = socket;
        this.publisher = publisher;
    }

    @Override
    public void run() {
        ObjectOutputStream out;
        ObjectInputStream in;
        try {
            // Initialising input and output streams
            out = new ObjectOutputStream(this.socket.getOutputStream());
            in = new ObjectInputStream(this.socket.getInputStream());
            Connection connection = new Connection(socket, in, out);

            while (!socket.isClosed() && !Thread.interrupted()) {
                // receiving an action string from the broker
                String action = in.readUTF();
                // if the requested action is a pull action
                if (action.equals("push")) {
                    // read the topic
                    String topic = in.readUTF();
                    ChannelName cn = publisher.getChannelName();
                    // if it is a hashtag
                    if (topic.startsWith("#")) {
                        // filenames to push
                        HashSet<String> toSend = new HashSet<>();

                        synchronized (publisher) {
                            // for every hashtag in the user's videos
                            for (String filename : cn.userVideoFilesMap.keySet()) {
                                // Search for the hashtag in the hashtags that concern this video
                                Value sample = cn.userVideoFilesMap.get(filename).get(0);

                                /*
                                 * If the required hashtag is found, then we add the video name in the list of
                                 * videos to push
                                 */
                                if (sample.videoFile.associatedHashtags.contains(topic)) {
                                    toSend.add(filename);
                                }
                            }
                        }
                        // if no videos of the required topic are found, send -1 as an error code
                        if (toSend.isEmpty()) {
                            out.writeInt(-1);
                            out.flush();
                        } else {
                            publisher.push(connection, toSend);
                        }
                        // search hashtags
                    } else {
                        // if it's a channel name, every video of the publisher is pushed
                        synchronized (publisher) {
                            if (cn.channelName.equals(topic)) {
                                if (!cn.userVideoFilesMap.isEmpty()) {
                                    HashSet<String> videos = new HashSet<>(cn.userVideoFilesMap.keySet());
                                    publisher.push(connection, videos);
                                } else {
                                    // Error code if the channel doesn't have any videos
                                    out.writeInt(-1);
                                    out.flush();
                                }
                            } else {
                                // Error code if the channel name is not this Publisher's
                                out.writeInt(-1);
                                out.flush();
                            }
                        }
                    }
                } else if (action.equals("notify")) {
                    int exitCode = -1;
                    String topic = in.readUTF();
                    ChannelName cn = publisher.getChannelName();

                    if (topic.startsWith("#")) {
                        synchronized (publisher) {
                            for (String i : cn.userVideoFilesMap.keySet()) {
                                // If there are videos that the Broker can pull related to this hashtag
                                if (cn.userVideoFilesMap.get(i).get(0).videoFile.associatedHashtags
                                        .contains(topic)) {
                                    exitCode = 0;
                                }
                            }
                        }
                    } else {
                        if (cn.channelName.equals(topic)) {
                            // If there are videos of the current channel that the Broker can pull
                            synchronized (publisher) {
                                if (!cn.userVideoFilesMap.isEmpty()) {
                                    exitCode = 0;
                                }
                            }
                        }
                    }
                    out.writeInt(exitCode);
                    out.flush();
                } else if (action.equals("brokerChange")) {
                    Broker broker = (Broker) in.readObject();
                    HashSet<String> hashtags = (HashSet<String>) in.readObject();

                    synchronized (publisher) {
                        // Remove the broker from the list
                        publisher.getBrokers().remove(broker);

                        // Find new broker
                        hashtags.forEach(hashtag -> publisher.notifyBrokersForHashTags(hashtag, true));
                    }
                } else if (action.equals("end")) {
                    break;
                }
            }
            in.close();
            out.close();
            socket.close();
        } catch (IOException io) {
            System.out.println(TAG + "Error in input or output: " + io.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
