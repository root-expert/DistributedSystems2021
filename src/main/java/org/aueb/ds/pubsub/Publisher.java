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

    public Publisher(String ip, int port) {
        // connect(ip, port);
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
        if (channelName.userVideoFilesMap.containsKey(filename)) {// if the video is already contained in the channel
                                                                  // name's video hashmap then it is already chunked
            video = channelName.userVideoFilesMap.get(filename);
        } else {// if not we parse the video from scratch
            try {
                ParseContext context = new ParseContext();// Tika's contect parser
                ContentHandler han = new BodyContentHandler();//
                Metadata data = new Metadata();// The metadata object to ectract the Value classs' attributes
                FileInputStream stream = new FileInputStream(new File(filename));// The byte stream to read the .mp4
                                                                                 // file
                Parser parser = new AutoDetectParser();
                parser.parse(stream, han, data, context);// Parsing the data
                byte[] fullvideo = stream.readAllBytes();// Exctract all bytes from the .mp4 file
                int len = fullvideo.length;
                int bins = Math.floorDiv(len, 10 * 1024);// calculate the number of 10KB full bins
                video = new ArrayList<Value>();
                Value videoChunk = new Value();
                byte[] chunk = null;// initialising the cunck arraylist as well as the temporary variables to
                                    // construct the videofile objects in
                for (int currentbin = 0; currentbin < bins; currentbin++) {// Fill each new Value with the corresponding
                                                                           // part of the full video array
                    chunk = new byte[100240];
                    for (int cByte = 0; cByte < 100240; cByte++) {
                        chunk[cByte] = fullvideo[cByte + currentbin * 10240];// Map the correct interval of the full
                                                                             // video array to copy to the chunk
                    }
                    // TODO fill in Value metadata
                    // Exctract name
                    videoChunk.videoFile.channelName = this.channelName.channelName;
                    // Extract date
                    // Ectract length
                    // Exctract framerate
                    // Extract frame Width
                    videoChunk.videoFile.videoFileChunk = chunk;// Create the Value objects and add them to the video
                                                                // ArrayList
                    video.add(videoChunk);
                    videoChunk = new Value();
                }
                int remanining = len - (bins * 10240);// Calculate the remaining rogue bytes, if any and create a final
                                                      // byte[] with less than 10240 bytes to house them, and follow the
                                                      // same procedure
                if (remanining != 0) {
                    chunk = new byte[10240];// the chunks have to be of equal size
                    for (int cByte = 0; cByte < remanining; cByte++) {
                        chunk[cByte] = fullvideo[bins * 10240 + cByte];
                    }
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
                channelName.userVideoFilesMap.put(filename, video);// Add chunked viedo in the channel name video
                                                                   // hashmap for later use,and return the hashed video
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
            connection.out.writeUTF("connectP");// Send a message to the corresponding
            connection.out.writeObject(this);
            ;
            // String received=connection.in.readUTF();
            // if (!received.equals("complete")){
            // throw new Exception("Error: action not completed in broker");
            // }

        } catch (IOException io) {
            System.out.println("Error in input/output when sending connection messages");
        }
        // catch(Exception e){
        // System.out.println(e.getMessage());
        // }
        return connection;
    }

    @Override
    public void disconnect(Connection connection) {
        try {
            connection.out.writeUTF("disconnectP");// Send a disconnect message to your corresponding broker, which it
                                                   // will propagate to the other brokers
            connection.out.writeUTF(channelName.channelName);
            // String received=connection.in.readUTF();//Await a response that the object is safely removed from the broker 
            // has been disconnected from the brokers succesfully
            // if (!received.equals("complete")){
            // throw new Exception("Error: action not completed in broker");
            // }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        super.disconnect(connection);
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(config.getPublisherPort());
            System.out.println("Hello");
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
                in = new ObjectInputStream(this.socket.getInputStream());// Initialising input and output streams
                String action = in.readUTF();// recieving an action string from the broker
                if (action.equals("push")) {// if the requested action is a pull action
                    String topic = in.readUTF();// read the topic
                    ChannelName cn = publisher.channelName;
                    if (topic.startsWith("#")) {// if it is a hashtag
                        // topic=topic.substring(1);//remove it in order to search in the hashmap
                        // out.writeUTF("received hashtag "+topic);
                        // out.flush();

                        ArrayList<String> toSend = new ArrayList<String>();// filenames to push
                        for (String filename : cn.userVideoFilesMap.keySet()) {// for every hashtag in the user's videos
                            Value sample = cn.userVideoFilesMap.get(filename).get(0);// Search for the hashtag in the
                                                                                     // hashtags that concern this video
                            if (sample.videoFile.associatedHashtags.contains(topic)) {// If the required hashtag is
                                                                                      // found then we add the video
                                                                                      // name in the list of videos to
                                                                                      // push
                                toSend.add(filename);
                            }
                        }
                        if (toSend.isEmpty()) {// if no videos of the required topic are found, send -1 as an error code
                            out.writeInt(-1);
                            out.flush();
                        } else {
                            out.writeInt(0);//if videos are to be sent send 0 as a success code
                            out.flush();
                            for (String filename : toSend) {// Push every chunk of the video
                                for (Value chunk : cn.userVideoFilesMap.get(filename)) {
                                    publisher.push(topic, chunk);
                                }
                            }
                        }
                        // search hastags
                    } else {
                        // out.writeUTF("received channel name "+topic);
                        //out.flush();
                        if (cn.channelName.equals(topic)) {// if it's a channel name, every video of the publisher is
                                                           // pushed
                            if (!cn.userVideoFilesMap.isEmpty()) {
                                out.writeInt(0);//if videos are to be sent send 0 as a success code
                                out.flush();
                                for (String filename : cn.userVideoFilesMap.keySet()) {
                                    for (Value videoChunk : cn.userVideoFilesMap.get(filename)) {
                                        publisher.push(topic, videoChunk);
                                    }
                                }
                            } else {
                                out.writeInt(-1);// Error code if the channel doesn't have any videos
                                out.flush();
                            }
                        } else {
                            out.writeInt(-1); // Error code if the channel name is not this Publisher's
                            out.flush();
                        }
                    }
                    // try{//for Push
                    // ArrayList<Value> videoChuncked=publisher.generateChunks(filename);
                    // int length=videoChuncked.size();
                    // out.writeInt(length);
                    // for (Value i:videoChuncked){
                    // out.writeObject(i);
                    // }
                    // // String received=in.readUTF();
                    // // if (!received.equals("complete")){
                    // // throw new Exception("Error incomplete send");
                    // // }
                    // }catch(Exception e){
                    // System.out.println("Error : "+e.getMessage());
                    // }
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
