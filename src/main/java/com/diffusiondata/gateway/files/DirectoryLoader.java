package com.diffusiondata.gateway.files;

import java.io.File;
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

        //final File[] files = dir.toFile().listFiles(File::isFile);
        Stream<Path> paths;
        try {
            paths = Files.walk(dir).filter(Files::isRegularFile);
        }
        catch(IOException ex) {
            ex.printStackTrace();
            return Stream.empty();
        }

        if(paths != null) {
            return paths.map(path -> {
                try {
                    final String name = path.toString().substring(dir.toString().length() + 1);
                    String content = new String(Files.readAllBytes(path));
                    return new UpdateEvent(name, content);
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
