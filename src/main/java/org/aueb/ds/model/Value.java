package org.aueb.ds.model;

import java.io.Serializable;
import java.util.HashSet;

public class Value implements Serializable, Comparable<Value> {

    public VideoFile videoFile;

    /**
     * Constructor
     *
     * @param name     String video name
     * @param channel  String channel name
     * @param date     String Creation date of the video
     * @param duration String video duration
     * @param rate     String video frame rate
     * @param height   String video display height in pixels
     * @param width    String video display width in pixels
     * @param hashtag  HashSet<String> hashtags that are relevant to the video
     * @param video    byte[] bytes of video
     */
    public Value(String name, String channel, String date, String duration, String rate, String height, String width,
            HashSet<String> hashtag, byte[] video) {
        videoFile = new VideoFile(name, channel, date, duration, rate, height, width, hashtag, video);
    }

    /**
     * Default Constructor
     */
    public Value() {
    }


    @Override
    /**
     * Compares Value objects based on videoName.
     *
     * @param v The Value object to compare to
     */
    public int compareTo(Value v) {
        return this.videoFile.videoName.compareTo(v.videoFile.videoName);
    }
}
