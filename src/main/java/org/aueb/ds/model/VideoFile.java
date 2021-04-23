package org.aueb.ds.model;

import java.io.Serializable;
import java.util.ArrayList;

public class VideoFile implements Serializable {

    String videoName;
    String channelName;
    String dateCreated;
    String length;
    String framerate;
    String frameWidth;
    String frameHeight;
    ArrayList<String> associatedHashtags;
    byte[] videoFileChunk;

}
