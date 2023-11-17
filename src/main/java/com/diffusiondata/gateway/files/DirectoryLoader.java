package com.diffusiondata.gateway.files;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class DirectoryLoader {

    private final Path dir;

    public DirectoryLoader(Path dir) {
        this.dir = dir;
    }

    public Stream<UpdateEvent> eventStream() {
        if(! dir.toFile().exists()) {
            return Stream.empty();
        }

        final File[] files = dir.toFile().listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile();
            }
        });

        if(files != null) {
            return Arrays.stream(files).map(file -> {
                try {
                    String topicPath = file.getName();
                    String content = new String(Files.readAllBytes(file.toPath()));
                    return new UpdateEvent(topicPath, content);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    return null;
                }
            }).filter(Objects::nonNull);
        }
        else {
            return Stream.empty();
        }
    }

}
