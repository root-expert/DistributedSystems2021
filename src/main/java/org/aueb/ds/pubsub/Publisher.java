package org.aueb.ds.pubsub;



import org.aueb.ds.model.ChannelName;
import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.util.Hashing;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import com.drew.imaging.mp4.Mp4MetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

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
    public ArrayList<Value> generateChunks(String filename) {
        ArrayList<Value> video = null;
        final int chunkSize = 10 * 1024;

        // if the video is already contained in the channel name's video hashmap then it is already chunked
        if (channelName.userVideoFilesMap.containsKey(filename))
            return channelName.userVideoFilesMap.get(filename);

        try {
            //Generate the proper filename to use for the File class
            String tempName=filename;
            if (!filename.contains(".mp4")){
                tempName+=".mp4";
            }

            // The byte stream to read the .mp4 file
            File file = new File(tempName);
            InputStream stream = new FileInputStream(file);
            // The metadata extractor from com.drew
            Metadata data=Mp4MetadataReader.readMetadata(stream);
            /* Store metadata information in a hashmap for easier access.
            * metadata consist of Directories which are larger collections of data like actual content, file specifiations etc.
            * and of Tags which are entries depicting actual data. Their format is Tag={Name,Description}.
            * Based on this description the hashmap is <Directory name,<Tag name, Tag String Description>.
            */
            HashMap<String,HashMap<String,String>> dirs=new HashMap<String,HashMap<String,String>>();
            HashMap<String,String> aux=null;
            for (Directory meta:data.getDirectories()){
                aux=new HashMap<String,String>();
                for (Tag tag:meta.getTags()){
                    aux.put(tag.getTagName(), tag.getDescription());
                }
                dirs.put(meta.getName(),aux);
            }
            aux=null;
            // Extract all bytes from the .mp4 file
            RandomAccessFile raf=new RandomAccessFile(file, "r");
            byte[] fullVideo = new byte[(int) file.length()];
            int len = raf.read(fullVideo);

            // calculate the number of 10KB full bins
            int bins = Math.floorDiv(len, chunkSize);

            /* initialising the chunk arraylist as well as the temporary variables to
             * construct the videoFile objects in
             */
            video = new ArrayList<>();

            Value videoChunk = new Value();

            byte[] chunk = null;

            // Fill each new Value with the corresponding part of the full video array
            for (int currentbin = 0; currentbin < bins; currentbin++) {
                chunk = new byte[chunkSize];

                // Map the correct interval of the full video array to copy to the chunk
                for (int cByte = 0; cByte < chunkSize; cByte++) {
                    chunk[cByte] = fullVideo[cByte + currentbin * chunkSize];
                }
                // Create the Value objects and add them to the video ArrayList
                videoChunk.videoFile.videoName=tempName.replace(".mp4","");
                videoChunk.videoFile.channelName =this.channelName.channelName;
                videoChunk.videoFile.length=dirs.get("MP4").get("Duration");
                videoChunk.videoFile.dateCreated=dirs.get("MP4").get("Creation Time");
                videoChunk.videoFile.frameHeight=dirs.get("MP4 Video").get("Height");
                videoChunk.videoFile.frameWidth=dirs.get("MP4 Video").get("Width");
                videoChunk.videoFile.framerate=dirs.get("MP4 Video").get("Frame Rate");
                videoChunk.videoFile.videoFileChunk = chunk;
                video.add(videoChunk);
                videoChunk = new Value();
            }
            /* Calculate the remaining rogue bytes, if any and create a final byte[] with less than 10240 bytes
             * to house them, and follow the
             * same procedure
             */
            int remaining = len - (bins * chunkSize);
            if (remaining > 0) {
                // the chunks have to be of equal size
                chunk = new byte[chunkSize];
                for (int cByte = 0; cByte < remaining; cByte++) {
                    chunk[cByte] = fullVideo[bins * chunkSize + cByte];
                }
                // Create the Value objects and add them to the video ArrayList
                videoChunk.videoFile.videoName=tempName.replace(".mp4","");
                videoChunk.videoFile.channelName =this.channelName.channelName;
                videoChunk.videoFile.length=dirs.get("MP4").get("Duration");
                videoChunk.videoFile.dateCreated=dirs.get("MP4").get("Creation Time");
                videoChunk.videoFile.frameHeight=dirs.get("MP4 Video").get("Height");
                videoChunk.videoFile.frameWidth=dirs.get("MP4 Video").get("Width");
                videoChunk.videoFile.framerate=dirs.get("MP4 Video").get("Frame Rate");
                videoChunk.videoFile.videoFileChunk = chunk;
                video.add(videoChunk);
            }
            fullVideo = null;
            // Add chunked viedo in the channel name video hashmap for later use,and return the hashed video
            channelName.userVideoFilesMap.put(filename, video);
            raf.close();
            stream.close();
        } catch (FileNotFoundException f) {
            System.out.println("Error: could not find file: " + f.getMessage());
        }catch (IOException io) {
            System.out.println("Error: problem during input/output: " + io.getMessage());
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
            // Send a message to the corresponding Broker and the Publisher object to be added in its hashmap
            connection.out.writeUTF("connectP");
            connection.out.flush();
            connection.out.writeObject(this);
            connection.out.flush();
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
            connection.out.flush();
            //send channel name to let the broker know which publisher to remove
            connection.out.writeUTF(channelName.channelName);
            connection.out.flush();
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
                            if (sample.videoFile.associatedHashtags.contains(topic)) {
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
                        // search hashtags
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
                }
                in.close();
                out.close();
            } catch (IOException io) {
                System.out.println("Error in input or output: " + io.getMessage());
            }
        }
    }
}
