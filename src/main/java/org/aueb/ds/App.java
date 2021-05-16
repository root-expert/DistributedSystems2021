package org.aueb.ds;

import org.aueb.ds.model.config.AppNodeConfig;
import org.aueb.ds.model.config.BrokerConfig;
import org.aueb.ds.pubsub.Broker;
import org.aueb.ds.pubsub.Consumer;
import org.aueb.ds.pubsub.Publisher;
import org.aueb.ds.util.ConfigParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

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
                pubThread.setName("publisher-thread");
                consThread.setName("consumer-thread");

                consThread.start();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                pubThread.start();

                // Menu
                new Thread(() -> {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    while (true) {
                        System.out.println();
                        System.out.println("[1] Subscribe");
                        System.out.println("[2] Unsubscribe");
                        System.out.println("[3] Upload Video");
                        System.out.println("[4] Remove Video");
                        System.out.println("[5] Exit");

                        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                        int ans;
                        try {
                            do {
                                System.out.print("Choose a number for your action: ");
                                ans = Integer.parseInt(in.readLine());
                            } while (ans < 1 || ans > 5);

                            if (ans == 1) {
                                boolean subscribed = false;

                                while (!subscribed) {
                                    System.out.print(Consumer.TAG + "Please enter a topic to subscribe: ");
                                    String topic = in.readLine();
                                    if (topic.equals("")) {
                                        System.out.print(
                                                "Are you sure you do not want to subscribe to a new topic? (y/n):");
                                        String answer = in.readLine();
                                        while (!answer.equals("y") && !answer.equals("n")) {
                                            System.out.print("Please enter y or n: ");
                                            answer = in.readLine();
                                        }
                                        if (answer.equals("n"))
                                            continue;
                                        break;
                                    }
                                    Broker selected;
                                    synchronized (consumer) {
                                        selected = consumer.findBroker(topic);
                                    }
                                    subscribed = consumer.subscribe(selected, topic);

                                    if (!subscribed) {
                                        System.out.print(
                                                Consumer.TAG + "We couldn't subscribe you to the specified topic. "
                                                        + "Do you want to pick a different one? (y/n): ");
                                        String answer = in.readLine();

                                        while (!answer.equals("y") && !answer.equals("n")) {
                                            System.out.print("Please enter y or n: ");
                                            answer = in.readLine();
                                        }

                                        if (answer.equals("n"))
                                            break;
                                    }
                                }
                            } else if (ans == 2) {
                                for (String topic : consumer.getSubscribedItems()) {
                                    System.out.println("* " + topic);
                                }
                                System.out.print(Consumer.TAG + "Please enter a topic to unsubscribe: ");
                                String topic = in.readLine();
                                if (topic.equals("")) {
                                    System.out.println("There was no input, canceling...");
                                    continue;
                                }
                                Broker selected;
                                synchronized (consumer) {
                                    selected = consumer.findBroker(topic);
                                }
                                consumer.unsubscribe(selected, topic);
                            } else if (ans == 3) {
                                File cwd = new File(System.getProperty("user.dir"));
                                for (File file : cwd.listFiles()) {
                                    String name = file.getName().replace(".mp4", "").split("#")[0];
                                    if (file.getName().contains(".mp4")
                                            && !publisher.getChannelName().userVideoFilesMap.containsKey(name))
                                        System.out.println("* " + name);
                                }
                                System.out.print(
                                        Publisher.TAG + "Please enter the name of the video you want to upload: ");
                                String filename = in.readLine();
                                if (filename.equals("")) {
                                    System.out.println("There was no input, canceling...");
                                    continue;
                                }
                                cwd = new File(System.getProperty("user.dir"));
                                for (File file : cwd.listFiles()) {
                                    if (file.getName().contains(".mp4") && file.getName().contains(filename))
                                        publisher.addVideo(file.getName());
                                }
                            } else if (ans == 4) {
                                System.out.println("\nYou can remove these videos");
                                for (String name : publisher.getChannelName().userVideoFilesMap.keySet()) {
                                    System.out.println("* " + name);
                                }
                                System.out.print(Publisher.TAG + "Please enter the name of video you want to remove: ");
                                String filename = in.readLine();
                                if (filename.equals("")) {
                                    System.out.println("There was no input, canceling...");
                                    continue;
                                }
                                publisher.removeVideo(filename);
                            } else {
                                System.exit(0);
                                break;
                            }

                            // in.close();
                        } catch (IOException io) {
                            io.printStackTrace();
                        }

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

                try {
                    pubThread.join();
                    consThread.join();
                } catch (InterruptedException ignored) {

                }
                break;
            default:
                System.out.println("Invalid arguments");
                break;
        }
    }
}
