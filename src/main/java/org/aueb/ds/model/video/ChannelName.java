package org.aueb.ds.model.video;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ChannelName implements Serializable {

    public String channelName;
    public HashSet<String> hashtagsPublished;
    public HashMap<String, ArrayList<Value>> userVideoFilesMap;

    public ChannelName(String channelName) {
        this.channelName = channelName;
        this.hashtagsPublished = new HashSet<>();
        this.userVideoFilesMap = new HashMap<>();
    }
}
