package org.aueb.ds.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class ChannelName implements Serializable {

    public String channelName;
    public ArrayList<String> hashtagsPublished;
    public HashMap<String, ArrayList<Value>> userVideoFilesMap;

    public ChannelName(String channelName) {
        this.channelName = channelName;
        this.hashtagsPublished = new ArrayList<>();
        this.userVideoFilesMap = new HashMap<>();
    }
}
