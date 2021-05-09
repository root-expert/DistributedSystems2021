package org.aueb.ds.pubsub;

import org.aueb.ds.model.ChannelName;
import org.aueb.ds.model.Connection;
import org.aueb.ds.model.Value;
import org.aueb.ds.model.VideoFile;
import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.util.Hashing;
import org.aueb.ds.util.MetadataExtract;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Publisher extends AppNode implements Runnable, Serializable {

    private static final long serialVersionUID = -6645374596536043061L;

    private ChannelName channelName;
    private final String lock = "";
    private static final String TAG = "[Publisher] ";

    protected AppNodeConfig config;

    public Publisher() {

    }

    public Publisher(AppNodeConfig conf) {
        this.config = conf;
    }

    @Override
    public void init() {
        //Intitialise the channel name object 
        channelName = new ChannelName(config.getChannelName());

        //Find all the videos on the Publisher's folder
        File cwd=new File(System.getProperty("user.dir"));
        for (File file:cwd.listFiles()){
            if(file.getName().contains(".mp4")){
                ArrayList<Value> video = generateChunks(file.getName());
                channelName.userVideoFilesMap.put(file.getName().replace(".mp4", "").split("#")[0], video);
                for (String h : video.get(0).videoFile.associatedHashtags) {
                    if (!channelName.hashtagsPublished.contains(h)) {
                        addHashTag(h);
                    }
                }
            }
        }

        addHashTag(channelName.channelName);
        System.out.println(TAG + "Videos added");
    }

    /**
     * Add hashtag or channelName to ChannelName's List ("#viral").
     * Inform Brokers.
     *
     * @param topic HashTag or channelName added.
     */
    public synchronized void addHashTag(String topic) {
        channelName.hashtagsPublished.add(topic);
        notifyBrokersForHashTags(topic, true);
        System.out.println(TAG + "Topic added: " + topic);
    }

    /**
     * Remove hashtag from ChannelName's List ("#viral").
     * Inform Brokers if hashtag is not used in any other Publisher's video.
     *
     * @param hashtag HashTag removed.
     */
    public synchronized void removeHashTag(String hashtag) {
        int hashtagCount = 0;
        for (ArrayList<Value> list : channelName.userVideoFilesMap.values()) {
            if (list.get(0).videoFile.associatedHashtags.contains(hashtag)) {
                hashtagCount++;
            }
        }
        if (hashtagCount == 1) {
            notifyBrokersForHashTags(hashtag, false);
            channelName.hashtagsPublished.remove(hashtag);
            System.out.println(TAG + "Topic removed: " + hashtag);
        }
    }

    /**
     * Hashes the specified topic with MD5 algo.
     *
     * @param topic Topic to hash.
     * @return The broker which is responsible for the specified topic.
     */
    public Broker hashTopic(String topic) {
        String hashedTopic = new Hashing().md5Hash(topic);

        ArrayList<Broker> brokers = this.getBrokers();
        Collections.sort(brokers);
        Broker selected = null;

        for (Broker broker : brokers) {
            switch (hashedTopic.compareTo(broker.hash)) {
                case -1:
                case 0:
                    selected = broker;
                    break;
                case 1:
            }
        }

        if (selected == null)
            selected = brokers.get(new Random().nextInt(brokers.size()));

        return selected;
    }

    /**
     * Pushes data to the specified topic
     *
     * @param connection The Connection object to send data on top of it.
     * @param videos     Videos to be send.
     */
    public void push(Connection connection, HashSet<String> videos) throws IOException {
        /*
         * if there are videos to be sent send 0 as a success code. Also send how many
         * video we are ready to send.
         */
        connection.out.writeInt(0);
        connection.out.writeInt(videos.size());
        connection.out.flush();

        synchronized (lock) {
            for (String filename : videos) {
                ArrayList<Value> chunks = channelName.userVideoFilesMap.get(filename);
                // Send the number of chunks
                connection.out.writeInt(chunks.size());
                for (Value chunk : chunks)
                    connection.out.writeObject(chunk);

                connection.out.flush();
            }
        }
    }

    public ChannelName getChannelName() {
        return channelName;
    }

    /**
     * Notifies about a failed push operation.
     *
     * @param broker The Broker to notify.
     */
    public void notifyFailure(Broker broker) {
        Connection connection = null;

        try {
            connection = super.connect(broker.config.getIp(), broker.config.getPort());
            connection.out.writeUTF("PushFailed");
            connection.out.writeUTF(channelName.channelName);

            connection.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    /**
     * Notifies the Broker about new content (hashtag)
     *
     * @param hashtag The hashtag to be notified.
     */
    public void notifyBrokersForHashTags(String hashtag, boolean add) {
        Broker broker = hashTopic(hashtag);
        System.out.println(broker);
        Connection connection = connect(broker.config.getIp(), broker.config.getPort());

        try {
            if (add) {
                connection.out.writeUTF("AddHashTag");
            } else {
                connection.out.writeUTF("RemoveHashTag");
            }
            connection.out.writeUTF(hashtag);
            connection.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
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
        final int chunkSize = 512 * 1024;

        try {
            // Generate the proper filename to use for the File class
            String tempName = filename;
            if (!filename.contains(".mp4")) {
                tempName += ".mp4";
            }
            // Seperate the name from the hashtags
            String[] args = tempName.replace(".mp4", "").split("#");
            HashSet<String> hashtags = new HashSet<String>();
            String name = args[0];
            for (int i = 1; i < args.length; i++) {
                hashtags.add("#" + args[i]);
            }
            MetadataExtract metadata = new MetadataExtract(tempName);

            File file = new File(tempName);
            // Extract all bytes from the .mp4 file
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            byte[] fullVideo = new byte[(int) file.length()];
            int len = raf.read(fullVideo);

            // calculate the number of 10KB full bins
            int bins = Math.floorDiv(len, chunkSize);

            /*
             * initialising the chunk arraylist as well as the temporary variables to
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
                videoChunk.videoFile = new VideoFile(name + "_" + currentbin, this.channelName.channelName,
                        metadata.getAttr("dateCreated"), metadata.getAttr("length"), metadata.getAttr("frameRate"),
                        metadata.getAttr("frameHeight"), metadata.getAttr("frameWidth"), hashtags, chunk);
                video.add(videoChunk);
                videoChunk = new Value();
            }
            /*
             * Calculate the remaining rogue bytes, if any and create a final byte[] with
             * less than 10240 bytes to house them, and follow the same procedure
             */
            int remaining = len - (bins * chunkSize);
            if (remaining > 0) {
                // the chunks have to be of equal size
                chunk = new byte[remaining];
                for (int cByte = 0; cByte < remaining; cByte++) {
                    chunk[cByte] = fullVideo[bins * chunkSize + cByte];
                }
                // Create the Value objects and add them to the video ArrayList
                videoChunk.videoFile = new VideoFile(name + "_" + bins, this.channelName.channelName,
                        metadata.getAttr("dateCreated"), metadata.getAttr("length"), metadata.getAttr("frameRate"),
                        metadata.getAttr("frameHeight"), metadata.getAttr("frameWidth"), hashtags, chunk);
                video.add(videoChunk);
            }
            raf.close();
        } catch (FileNotFoundException f) {
            System.out.println("Error: could not find file: " + f.getMessage());
        } catch (IOException io) {
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
        Connection connection = null;
        try {
            connection = super.connect(ip, port);
            // Send a message to the corresponding Broker and the Publisher object to be
            // added in its hashmap
            connection.out.writeUTF("connectP");
            connection.out.writeObject(this);
            connection.out.flush();
        } catch (IOException io) {
            System.out.println(TAG + "Error in input/output when sending connection messages");
            io.printStackTrace();
        }
        return connection;
    }

    @Override
    public void disconnect(Connection connection) {
        try {
            /*
             * Send a disconnect message to your corresponding broker, which it will
             * propagate to the other brokers
             */
            connection.out.writeUTF("disconnectP");
            // send channel name to let the broker know which publisher to remove
            connection.out.writeUTF(channelName.channelName);
            connection.out.flush();
        } catch (Exception e) {
            System.out.println(TAG + "Error while sending disconnection message " + e.getMessage());
            e.printStackTrace();
        } finally {
            super.disconnect(connection);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Publisher publisher = (Publisher) o;
        return channelName.channelName.equals(publisher.channelName.channelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName.channelName);
    }

    @Override
    public void run() {
        init();

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

                // receiving an action string from the broker
                String action = in.readUTF();
                // if the requested action is a pull action
                if (action.equals("push")) {
                    // read the topic
                    String topic = in.readUTF();
                    ChannelName cn = publisher.channelName;
                    // if it is a hashtag
                    if (topic.startsWith("#")) {
                        // filenames to push
                        HashSet<String> toSend = new HashSet<>();

                        synchronized (publisher.lock) {
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
                        synchronized (publisher.lock) {
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
                    ChannelName cn = publisher.channelName;

                    if (topic.startsWith("#")) {
                        synchronized (publisher.lock) {
                            for (String i : cn.userVideoFilesMap.keySet()) {
                                // If there are videos that the Broker can pull related to this hashtag
                                if (cn.userVideoFilesMap.get(i).get(0).videoFile.associatedHashtags.contains(topic)) {
                                    exitCode = 0;
                                }
                            }
                        }
                        if (exitCode == -1) {
                            /*
                             * Searches into it's designated file for new videos, so that is can check in
                             * the latest data if it finds new videos it splits them into chunks and adds
                             * them into the user's available videos
                             */
                            File folder = new File(System.getProperty("user.dir"));

                            synchronized (publisher.lock) {
                                for (File f : folder.listFiles()) {
                                    if (f.getName().contains(".mp4")) {
                                        if (!publisher.channelName.userVideoFilesMap
                                                .containsKey(f.getName().replace(".mp4", "").split("#")[0])) {
                                            // Add chunked video in the channel name video hashmap for later use,and return
                                            // the hashed video
                                            ArrayList<Value> video = publisher.generateChunks(f.getName());
                                            publisher.channelName.userVideoFilesMap
                                                    .put(f.getName().replace(".mp4", "").split("#")[0], video);
                                            for (String h : video.get(0).videoFile.associatedHashtags) {
                                                if (!publisher.channelName.hashtagsPublished.contains(h)) {
                                                    publisher.addHashTag(h);
                                                }
                                            }
                                            if (f.getName().contains(topic)) {
                                                exitCode = 0;
                                            }
                                        } else {
                                            exitCode = 0;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        if (cn.channelName.equals(topic)) {
                            // If there are videos of the current channel that the Broker can pull
                            synchronized (publisher.lock) {
                                if (!cn.userVideoFilesMap.isEmpty()) {
                                    exitCode = 0;
                                } else {
                                    /*
                                     * Searches into it's designated file for new videos, so that is can check in
                                     * the latest data if it finds new videos it splits them into chunks and adds
                                     * them into the user's available videos
                                     */
                                    File folder = new File(System.getProperty("user.dir"));
                                    // TODO: assign appropriate file
                                    for (File f : folder.listFiles()) {
                                        if (f.getName().contains(".mp4")) {
                                            if (!publisher.channelName.userVideoFilesMap
                                                    .containsKey(f.getName().replace(".mp4", "").split("#")[0])) {
                                                // Add chunked viedo in the channel name video hashmap for later use,and
                                                // return
                                                // the hashed video
                                                ArrayList<Value> video = publisher.generateChunks(f.getName());
                                                publisher.channelName.userVideoFilesMap
                                                        .put(f.getName().replace(".mp4", "").split("#")[0], video);
                                                for (String h : video.get(0).videoFile.associatedHashtags) {
                                                    if (!publisher.channelName.hashtagsPublished.contains(h)) {
                                                        publisher.addHashTag(h);
                                                    }
                                                }
                                                exitCode = 0;
                                            }
                                        } else {
                                            exitCode = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    out.writeInt(exitCode);
                    out.flush();
                }
                in.close();
                out.close();
                socket.close();
            } catch (IOException io) {
                System.out.println("Error in input or output: " + io.getMessage());
            }
        }
    }
}
