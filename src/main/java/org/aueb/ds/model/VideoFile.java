package org.aueb.ds.model;

import java.io.Serializable;
import java.util.HashSet;

public class VideoFile implements Serializable {

    public String videoName;
    public String channelName;
    public String dateCreated;
    public String length;
    public String framerate;
    public String frameWidth;
    public String frameHeight;
    public HashSet<String> associatedHashtags;
    public byte[] videoFileChunk;

    /**
     * Constructor
     * 
     * @param name       String video name
     * @param channel    String channel name
     * @param date       String Creation date of the video
     * @param duration   String video duration
     * @param rate       String video frame rate
     * @param height     String video display height in pixels
     * @param width      String video display width in pixels
     * @param hashtag    HashSet<String> hashtags that are relevant to the video
     * @param video      byte[] bytes of video
     */
    public VideoFile(String name, String channel, String date, String duration, String rate, String height,
            String width, HashSet<String> hashtag, byte[] video) {
        this.videoName = name;
        this.channelName = channel;
        this.dateCreated = date;
        this.length = duration;
        this.framerate = rate;
        this.frameHeight = height;
        this.frameWidth = width;
        this.associatedHashtags = hashtag;
        this.videoFileChunk = video;

    }
}