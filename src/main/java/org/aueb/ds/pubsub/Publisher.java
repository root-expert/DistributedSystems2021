package org.aueb.ds.pubsub;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.aueb.ds.model.ChannelName;
import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.util.Hashing;
import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

public class Publisher extends AppNode implements Runnable, Serializable {

    private ChannelName channelName = new ChannelName(UUID.randomUUID().toString()); // TODO: Change me

    public Publisher(AppNodeConfig conf) {
        super(conf);
    }

    public void addHashTag(String hashtag) {

    }

    public void removeHashTag(String hashtag) {

    }

    public void getBrokerList() {

    }

    /**
     * Hashes the specified topic with MD5 algo.
     * 
     * @param topic Topic to hash.
     * @return The broker which is responsible for the specified topic.
     */
    public Broker hashTopic(String topic) {
        Hashing hashing = new Hashing();

        String hashedTopic = hashing.md5Hash(topic);

        ArrayList<Broker> brokers = this.getBrokers();
        Broker selected = null;
        for (Broker broker : brokers) {
            // TODO: Cover all cases
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
     * 
     * @param topic Topic to push data.
     * @param value Data to push.
     */
    public void push(String topic, Value value) {
    }

    /**
     * Notifies about a failed push operation.
     * 
     * @param broker The Broker to notify.
     */
    public void notifyFailure(Broker broker) {

    }

    /**
     * Notifies the Broker about new content (hashtag)
     *
     * @param hashtag The hashtag to be notified.
     */
    public void notifyBrokersForHashTags(String hashtag, boolean add) {

    }

    /**
     * Generate chunks of a Video file. It extracts Video's metadata using the
     * Apache Tika Library.
     *
     * @param filename The filename to open.
     * @return An ArrayList with all the chunks.
     */
    public ArrayList<Value> generateChunks(String filename) throws Exception {
        // TODO Metadata
        ArrayList<Value> video = null;
        final int chunkSise=10240;
        // if the video is already contained in the channel name's video hashmap then it is already chunked
        if (channelName.userVideoFilesMap.containsKey(filename)) {
            video = channelName.userVideoFilesMap.get(filename);
        } else {// if not we parse the video from scratch
            try {
                // Tika's contect parser
                ParseContext context = new ParseContext();
                ContentHandler han = new BodyContentHandler();
                // The metadata object to ectract the Value classs' attributes
                Metadata data = new Metadata();

                // The byte stream to read the .mp4 file
                FileInputStream stream = new FileInputStream(new File(filename));
                Parser parser = new AutoDetectParser();
                parser.parse(stream, han, data, context);// Parsing the data
                
                // Exctract all bytes from the .mp4 file
                byte[] fullvideo = stream.readAllBytes();
                int len = fullvideo.length;

                // calculate the number of 10KB full bins
                int bins = Math.floorDiv(len, chunkSise);

                /* initialising the cunck arraylist as well as the temporary variables to
                * construct the videofile objects in               
                */
                video = new ArrayList<Value>();
                
                Value videoChunk = new Value();
                
                byte[] chunk = null;

                // Fill each new Value with the corresponding part of the full video array
                for (int currentbin = 0; currentbin < bins; currentbin++) {
                    chunk = new byte[chunkSise];

                    // Map the correct interval of the full video array to copy to the chunk
                    for (int cByte = 0; cByte < chunkSise; cByte++) {
                        chunk[cByte] = fullvideo[cByte + currentbin * chunkSise];
                    }
                    // Create the Value objects and add them to the video ArrayList
                    // TODO fill in Value metadata
                    // Exctract name
                    videoChunk.videoFile.channelName = this.channelName.channelName;
                    // Extract date
                    // Ectract length
                    // Exctract framerate
                    // Extract frame Width
                    videoChunk.videoFile.videoFileChunk = chunk;
                    video.add(videoChunk);
                    videoChunk = new Value();
                }
                /** Calculate the remaining rogue bytes, if any and create a final byte[] with less than 10240 bytes
                * to house them, and follow the
                * same procedure
                */
                int remanining = len - (bins * chunkSise);
                if (remanining > 0) {
                    // the chunks have to be of equal size
                    chunk = new byte[chunkSise];
                    for (int cByte = 0; cByte < remanining; cByte++) {
                        chunk[cByte] = fullvideo[bins * chunkSise + cByte];
                    }
                    // Create the Value objects and add them to the video ArrayList
                    // TODO fill in Value metadata
                    // Exctract name
                    videoChunk.videoFile.channelName = this.channelName.channelName;
                    // Extract date
                    // Ectract length
                    // Exctract framerate
                    // Extract frame Width
                    videoChunk.videoFile.videoFileChunk = chunk;
                    video.add(videoChunk);
                }
                fullvideo = null;
                // Add chunked viedo in the channel name video hashmap for later use,and return the hashed video
                channelName.userVideoFilesMap.put(filename, video);
            } catch (FileNotFoundException e) {
                System.out.println("Error: the file was not found: " + e.getMessage());
            }

        }
        return video;
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
        Connection connection = super.connect(ip, port);
        try {
            // Send a message to the corresponding Broker and the Publisher object to be added in it's hashmap
            connection.out.writeUTF("connectP");
            connection.out.writeObject(this);
        } catch (IOException io) {
            System.out.println("Error in input/output when sending connection messages");
        }
        return connection;
    }

    @Override
    public void disconnect(Connection connection) {
        try {
            /* Send a disconnect message to your corresponding broker,
            * which it will propagate to the other brokers
            */
            connection.out.writeUTF("disconnectP");
            //send channel name to let the broker know which publisher to remove
            connection.out.writeUTF(channelName.channelName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        super.disconnect(connection);
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(config.getPublisherPort());
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
        private Socket socket;
        private Publisher publisher;

        public Handler(Socket socket, Publisher publisher) {
            this.socket = socket;
            this.publisher = publisher;
        }

        @Override
        public void run() {
            ObjectOutputStream out;
            ObjectInputStream in;
            try {
                out = new ObjectOutputStream(this.socket.getOutputStream());
                // Initialising input and output streams
                in = new ObjectInputStream(this.socket.getInputStream());
                // recieving an action string from the broker
                String action = in.readUTF();
                // if the requested action is a pull action
                if (action.equals("push")) {
                    // read the topic
                    String topic = in.readUTF();
                    ChannelName cn = publisher.channelName;
                    // if it is a hashtag
                    if (topic.startsWith("#")) {
                        // filenames to push
                        ArrayList<String> toSend = new ArrayList<String>();

                        // for every hashtag in the user's videos
                        for (String filename : cn.userVideoFilesMap.keySet()) {
                            // Search for the hashtag in the hashtags that concern this video
                            Value sample = cn.userVideoFilesMap.get(filename).get(0);

                            /* If the required hashtag is found, then we add the video 
                            * name in the list of videos topush
                            */
                            if (sample.videoFile.associatedHashtags.contains(topic)){
                                toSend.add(filename);
                            }
                        }
                        // if no videos of the required topic are found, send -1 as an error code
                        if (toSend.isEmpty()) {
                            out.writeInt(-1);
                            out.flush();
                        } else {
                            //if videos are to be sent send 0 as a success code
                            out.writeInt(0);
                            out.flush();

                            // Push every chunk of the video
                            for (String filename : toSend) {
                                for (Value chunk : cn.userVideoFilesMap.get(filename)) {
                                    publisher.push(topic, chunk);
                                }
                            }
                        }
                        // search hastags
                    } else {
                        // if it's a channel name, every video of the publisher is pushed
                        if (cn.channelName.equals(topic)) {
                            if (!cn.userVideoFilesMap.isEmpty()) {
                                //if videos are to be sent send 0 as a success code
                                out.writeInt(0);
                                out.flush();
                                //Push every video the channel has
                                for (String filename : cn.userVideoFilesMap.keySet()) {
                                    for (Value videoChunk : cn.userVideoFilesMap.get(filename)) {
                                        publisher.push(topic, videoChunk);
                                    }
                                }
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
                } else if (action.equals("notify")) {
                    String receive = in.readUTF();//
                    // TODO implement notify Publicers
                }
                in.close();
                out.close();
            } catch (IOException io) {
                System.out.println("Error in input or output: " + io.getMessage());
            }
        }
    }
}
