package org.aueb.ds.model;

import java.io.Serializable;
import java.util.ArrayList;

public class VideoFile implements Serializable {

    public String videoName;
    public String channelName;
    public String dateCreated;
    public String length;
    public String framerate;
    public String frameWidth;
    public String frameHeight;
    public ArrayList<String> associatedHashtags;
    public byte[] videoFileChunk;

}
