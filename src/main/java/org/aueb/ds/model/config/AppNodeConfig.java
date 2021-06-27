package org.aueb.ds.model.config;

public class AppNodeConfig extends Config {

    private String ip;
    private int publisherPort;
    private int consumerPort;
    private static final long serialVersionUID = 6529685098367747690L;

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

    @Override
    public String toString() {
        return "AppNodeConfig{" +
                "ip='" + ip + '\'' +
                ", publisherPort=" + publisherPort +
                ", consumerPort=" + consumerPort +
                '}';
    }
}
