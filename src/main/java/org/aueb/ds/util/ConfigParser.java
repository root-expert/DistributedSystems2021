package org.aueb.ds.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.aueb.ds.model.config.Config;

import java.io.File;
import java.io.IOException;

public class ConfigParser {

    public <T extends Config> T parseConfig(String file, Class<T> what) throws IOException {
        String path = System.getProperty("user.dir").concat("/").concat(file);

        File config = new File(path);
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        return objectMapper.readValue(config, what);
    }
}
