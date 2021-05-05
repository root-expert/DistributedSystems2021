package org.aueb.ds.model;

import java.io.Serializable;
import java.util.ArrayList;

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
     * @param hashtag  ArrayList<String> hashtags that are relevant to the video
     * @param video    byte[] bytes of video
     */
    public Value(String name, String channel, String date, String duration, String rate, String height, String width,
                 ArrayList<String> hashtag, byte[] video) {
        videoFile = new VideoFile(name, channel, date, duration, rate, height, width, hashtag, video);
    }

    /**
     * Default Constructor
     */
    public Value() {
    }

    ;


    @Override
    /**
     * Compares Value objects based on videoName.
     * Helps to order the chunk files.
     *
     * @param v The Value object to compare to
     */
    public int compareTo(Value v) {
        StringBuilder thisIndex = new StringBuilder();
        StringBuilder vIndex = new StringBuilder();
        boolean found = false;

        for (char c : this.videoFile.videoName.toCharArray()) {
            if (Character.isDigit(c)) {
                thisIndex.append(c);
                found = true;
            } else if (found) {
                break;
            }
        }
        found = false;

        for (char c : v.videoFile.videoName.toCharArray()) {
            if (Character.isDigit(c)) {
                vIndex.append(c);
                found = true;
            } else if (found) {
                break;
            }
        }
        return thisIndex.compareTo(vIndex);
    }
}
