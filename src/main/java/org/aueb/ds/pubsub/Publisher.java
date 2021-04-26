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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

public class Publisher extends AppNode implements Runnable {

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
     *
     * @param hashtag The hashtag to be notified.
     */
    public void notifyBrokersForHashTags(String hashtag, boolean add) {

    }

    /**
     * Generate chunks of a Video file.
     * It extracts Video's metadata using the Apache Tika Library.
     *
     * @param filename The filename to open.
     * @return An ArrayList with all the chunks.
     */
    public ArrayList<Value> generateChunks(String filename) throws Exception {
        //TODO Metadata 
        ArrayList<Value> video=null;
        if (channelName.userVideoFilesMap.containsKey(filename)){//if the video is already contained in the channel name's video hashmap then it is already chunked
            video=channelName.userVideoFilesMap.get(filename);
        }else{//if not we parse the video from scratch
            ParseContext context=new ParseContext();//Tika's contect parser
            ContentHandler han=new BodyContentHandler();//
            Metadata data=new Metadata();//The metadata object to ectract the Value classs' attributes
            FileInputStream stream=new FileInputStream(new File(filename));//The byte stream to read the .mp4 file
            Parser parser=new AutoDetectParser();
            parser.parse(stream, han, data, context);//Parsing the data
            //TODO fill in metadata using the metadata object
            byte[] fullvideo=stream.readAllBytes();//Exctract all bytes from the .mp4 file
            int len=fullvideo.length;
            int bins=Math.floorDiv(len, 10*1024);//calculate the number of 10KB full bins
            video=new ArrayList<Value>();
            Value videoChunk=new Value();
            byte[] chunk=null;//initialising the cunck arraylist as well as the temporary variables to construct the videofile objects in
            for(int currentbin=0;currentbin<bins;currentbin++){//Fill each new Value with the corresponding part of the full video array
                chunk=new byte[100240];
                for (int cByte=0;cByte<100240;cByte++){
                    chunk[cByte]=fullvideo[cByte+currentbin*10240];//Map the correct interval of the full video array to copy to the chunk
                }
                //TODO fill in Value metadata
                videoChunk.videoFile.videoFileChunk=chunk;//Create the Value objects and add them to the video ArrayList
                video.add(videoChunk);
                videoChunk=new Value();
            }
            int remanining=len-(bins*10240);//Calculate the remaining rogue bytes, if any and create a final byte[] with less than 10240 bytes to house them, and follow the same procedure 
            if (remanining!=0){
                chunk=new byte[remanining];
                for (int cByte=0;cByte<remanining;cByte++){
                    chunk[cByte]=fullvideo[bins*10240+cByte];
                }
                //TODO fill in Value metadata
                videoChunk.videoFile.videoFileChunk=chunk;
                video.add(videoChunk);
            }
            fullvideo=null;
            channelName.userVideoFilesMap.put(filename,video);//Add chunked viedo in the channel name video hashmap for later use,and return the hashed video
        }
        return video;
    }

    /**
     * Opens a connections to the specified IP and port
     * and sends registration messages.
     * @param ip The IP to open the connection.
     * @param port The port to open the connection.
     * @return A Connection object.
     */
    @Override
    public Connection connect(String ip, int port) {
        Connection connection = super.connect(ip, port);
        try {
            connection.out.writeUTF("connectP");//Send a message to the corresponding 
            // String received=connection.in.readUTF();
            // if (!received.equals("complete")){
            //     throw new Exception("Error: action not completed in broker");
            // }
        
        } catch (IOException io) {
            System.out.println("Error in input/output when sending connection messages");
        }
        // catch(Exception e){
        //     System.out.println(e.getMessage());
        // }
        return connection;
    }

    @Override
    public void disconnect(Connection connection) {
        try {
            connection.out.writeUTF("disconnectP");//Send a disconnect message to your corresponding broker, which it will propagate to the other brokers
            // String received=connection.in.readUTF();//Await a response that the object has been disconnected from the brokers succesfully
            // if (!received.equals("complete")){
            //     throw new Exception("Error: action not completed in broker");
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
            try {
                ObjectOutputStream out=new ObjectOutputStream(this.socket.getOutputStream());
                ObjectInputStream in =new ObjectInputStream(this.socket.getInputStream());
                String action=in.readUTF();
                if (action.equals("pull")){
                    String filename=in.readUTF();
                    try{
                        ArrayList<Value> videoChuncked=publisher.generateChunks(filename);
                        int length=videoChuncked.size();
                        out.writeInt(length);
                        for (Value i:videoChuncked){
                            out.writeObject(i);
                        }
                        String received=in.readUTF();
                        if (!received.equals("complete"));
                            throw new Exception("Error incomplete send");
                    }catch(Exception e){
                        //TODO: handle exception
                    }
                }else if(action.equals("notify")) {
                    String receive=in.readUTF();
                }
            } catch (IOException io) {
                //TODO: handle exception
            }
            
        }
    }
}
