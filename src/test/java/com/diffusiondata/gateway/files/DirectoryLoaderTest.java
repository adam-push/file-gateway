package com.diffusiondata.gateway.files;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class DirectoryLoaderTest {

    @TempDir
    Path tempDir;

    private DirectoryLoader loader;

    @BeforeEach
    public void setUp() {
        loader = new DirectoryLoader(tempDir);
    }

    @Test
    public void shouldReturnStreamOfEventsForFilesInDirectory() throws Exception {
        // Create some test files
        Files.writeString(tempDir.resolve("test1.txt"), "content1");
        Files.writeString(tempDir.resolve("test2.txt"), "content2");
        Files.createDirectory(Paths.get(tempDir.toString(), "sub"));
        Files.writeString(tempDir.resolve("sub/test3.txt"), "content3");

        // Test
        var events = loader.eventStream().collect(Collectors.toList());

        System.out.println("Events: " + events.size());
        // Assertions
        assertEquals(3, events.size());
        assertTrue(events.stream().anyMatch(event -> event.getName().equals("test1.txt")));
        assertTrue(events.stream().anyMatch(event -> event.getName().equals("test2.txt")));
        assertTrue(events.stream().anyMatch(event -> event.getName().equals("sub/test3.txt")));
    }

    @Test
    public void shouldReturnEmptyStreamForEmptyDirectory() {
        var events = loader.eventStream().collect(Collectors.toList());

        // Assertions
        assertTrue(events.isEmpty());
    }

    @Test
    public void shouldHandleNonExistentDirectory() {
        loader = new DirectoryLoader(tempDir.resolve("nonexistent"));

        // Assertions
        assert(loader.eventStream().findAny().isEmpty());
    }
}
