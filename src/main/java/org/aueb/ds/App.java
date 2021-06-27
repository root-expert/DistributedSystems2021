package org.aueb.ds;

import org.aueb.ds.model.config.BrokerConfig;
import org.aueb.ds.pubsub.Broker;
import org.aueb.ds.util.ConfigParser;

import java.io.IOException;

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
                brokerThread.setName("broker-thread");
                brokerThread.start();
                break;
            default:
                System.out.println("Invalid arguments");
                break;
        }
    }
}
