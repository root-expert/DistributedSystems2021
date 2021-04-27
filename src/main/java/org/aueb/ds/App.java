package org.aueb.ds;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
// import org.apache.tika.parser.mp4.MP4Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.model.config.BrokerConfig;
import org.aueb.ds.pubsub.Broker;
import org.aueb.ds.pubsub.Consumer;
import org.aueb.ds.pubsub.Publisher;
import org.aueb.ds.util.ConfigParser;
import org.xml.sax.SAXException;

public class App {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You need to provide at least one argument!");
            return;
        }

        String what = args[0];
        switch (what) {
            case "broker":
                BrokerConfig brokerConfig;
                try {
                    brokerConfig = new ConfigParser().parseConfig("broker.yml", BrokerConfig.class);
                } catch (IOException e) {
                    System.out.println("There was an error parsing broker.yml file! Exiting...");
                    e.printStackTrace();
                    return;
                }

                // start broker
                Broker broker = new Broker(brokerConfig);
                Thread brokerThread = new Thread(broker);
                brokerThread.start();
                break;
            case "node":
                AppNodeConfig appNodeConfig;
                try {
                    appNodeConfig = new ConfigParser().parseConfig("appnode.yml", AppNodeConfig.class);
                } catch (IOException e) {
                    System.out.println("There was an error parsing appnode.yml file! Exiting...");
                    e.printStackTrace();
                    return;
                }

                // start Publisher, Consumer threads
                Publisher publisher = new Publisher(appNodeConfig);
                Consumer consumer = new Consumer(appNodeConfig);

                Thread pubThread = new Thread(publisher);
                Thread consThread = new Thread(consumer);

                pubThread.start();
                consThread.start();

                try {
                    pubThread.join();
                    consThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case "test":
                //System.out.println("hello");
                // Parser test, Failed
                // try {
                //     ParseContext context=new ParseContext();//Tika's contect parser
                //     BodyContentHandler  han=new BodyContentHandler();//
                //     Metadata data=new Metadata();//The metadata object to ectract the Value classs' attributes
                //     FileInputStream stream=new FileInputStream(new File("temp.mp4"));//The byte stream to read the .mp4 file
                //     MP4Parser parser=new MP4Parser();
                //     parser.parse(stream, han, data, context);//Parsing the data
                //     String[] names=data.names();
                //     System.out.println(names.length);
                // } catch (FileNotFoundException e) {
                //     System.out.println("Error: in finding the correct file: "+e.getMessage());
                // }catch(IOException io){
                //     System.out.println("Error: in input/output: "+io.getMessage());
                // }catch(SAXException sax){
                //     System.out.println("Error: "+sax.getMessage());
                // }catch(TikaException tika){
                //     System.out.println("Error: "+tika.getMessage());
                // }
                // Thread pub= new Thread(new Publisher("127.0.0.1",4321));
                // pub.start();
                // new InnerBroker("anastasis").start();
                // try {
                //     Thread.sleep(1000);
                // } catch (Exception e) {
                //     System.out.println(e.getMessage());
                // }
                // new InnerBroker("#foryoupage").start();
                break;
            default:
                System.out.println("Invalid arguments");
                break;
        }
    }
    
}
