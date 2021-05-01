package org.aueb.ds.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import com.drew.imaging.mp4.Mp4MetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

/**
 * MetaDataExtract utility class to extract relevant metadata from an .mp4 video
 * and store them.
 */
public class MetadataExtract {
    HashMap<String, String> metadata;

    /**
     * Initialises the object and the hashmap which will hold the elements. The data
     * of the specified video are extracted and stored in aforementioned hashmap.
     * 
     * @param filename the string name of the video to be analysed.
     */
    public MetadataExtract(String filename) {
        metadata = new HashMap<>();
        try {
            // The byte stream to read the .mp4 file
            File file = new File(filename);
            InputStream stream = new FileInputStream(file);
            // The metadata extractor from com.drew
            Metadata data = Mp4MetadataReader.readMetadata(stream);
            /*
             * Store metadata information in a hashmap for easier access. metadata consist
             * of Directories which are larger collections of data like actual content, file
             * specifiations etc. and of Tags which are entries depicting actual data. Their
             * format is Tag={Name,Description}. Based on this description the hashmap is
             * <Directory name,<Tag name, Tag String Description>.
             */
            HashMap<String, HashMap<String, String>> dirs = new HashMap<>();
            HashMap<String, String> aux = null;
            for (Directory meta : data.getDirectories()) {
                aux = new HashMap<>();
                for (Tag tag : meta.getTags()) {
                    aux.put(tag.getTagName(), tag.getDescription());
                }
                dirs.put(meta.getName(), aux);
            }
            /**
             * Add the relevant data into the hashmap using the names of the corresponding
             * videoFile attributes
             */
            metadata.put("length", dirs.get("MP4").get("Duration"));
            metadata.put("dateCreated", dirs.get("MP4").get("Creation Time"));
            metadata.put("frameHeight", dirs.get("MP4 Video").get("Height"));
            metadata.put("frameWidth", dirs.get("MP4 Video").get("Width"));
            metadata.put("frameRate", dirs.get("MP4 Video").get("Frame Rate"));
            stream.close();
        } catch (IOException io) {
            System.out.println("Error in input/output: " + io.getMessage());
        }
    }

    /**
     * @return the hashmap of the extracted metadata
     */
    public HashMap<String, String> getMeta() {
        return metadata;
    }

    /**
     * Get a certain metadata attribute from the hashmap.
     * 
     * @param tag the name of the attribute we need the description of
     * @return String, the description of the requested metadata attribute
     * 
     */
    public String getAttr(String tag) {
        return metadata.get(tag);
    }

}