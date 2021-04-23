package org.aueb.ds.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class ChannelName implements Serializable {

    public String channelName;
    public ArrayList<String> hashtagsPublished;
    public HashMap<String, ArrayList<Value>> userVideoFilesMap;
}
