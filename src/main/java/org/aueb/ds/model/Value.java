package org.aueb.ds.model;

import java.io.Serializable;


public class Value implements Serializable {

    public VideoFile videoFile;
    public Value(){
        videoFile=new VideoFile();
    }
}
