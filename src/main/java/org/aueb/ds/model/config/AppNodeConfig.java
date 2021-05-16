package org.aueb.ds.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AppNodeConfig extends Config {

    private String ip;

    @JsonProperty("publisher_port")
    private int publisherPort;

    @JsonProperty("consumer_port")
    private int consumerPort;

    @JsonProperty("broker_ip")
    private String brokerIP;

    @JsonProperty("broker_port")
    private int brokerPort;

    @JsonProperty("channel_name")
    private String channelName;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPublisherPort() {
        return publisherPort;
    }

    public void setPublisherPort(int publisherPort) {
        this.publisherPort = publisherPort;
    }

    public int getConsumerPort() {
        return consumerPort;
    }

    public void setConsumerPort(int consumerPort) {
        this.consumerPort = consumerPort;
    }

    public String getBrokerIP() {
        return brokerIP;
    }

    public void setBrokerIP(String brokerIP) {
        this.brokerIP = brokerIP;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    @Override
    public String toString() {
        return "AppNodeConfig{" +
                "ip='" + ip + '\'' +
                ", publisherPort=" + publisherPort +
                ", consumerPort=" + consumerPort +
                ", brokerIP='" + brokerIP + '\'' +
                ", brokerPort=" + brokerPort +
                ", channelName='" + channelName + '\'' +
                '}';
    }
}
