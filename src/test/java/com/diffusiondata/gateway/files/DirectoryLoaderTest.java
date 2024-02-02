package com.diffusiondata.gateway.files;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
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

        // Test
        var events = loader.eventStream().collect(Collectors.toList());

        // Assertions
        assertEquals(2, events.size());
        assertTrue(events.stream().anyMatch(event -> event.getName().equals("test1.txt")));
        assertTrue(events.stream().anyMatch(event -> event.getName().equals("test2.txt")));
    }

    @Test
    public void shouldReturnEmptyStreamForEmptyDirectory() {
        var events = loader.eventStream().collect(Collectors.toList());

        // Assertions
        assertTrue(events.isEmpty());
    }

    @Test
    public void shouldIgnoreSubdirectories() throws Exception {
        // Create a file in this directory
        Files.writeString(tempDir.resolve("test.txt"), "content");

        // Create a subdirectory and a file
        Path subDir = Files.createDirectory(tempDir.resolve("subdir"));
        Files.writeString(subDir.resolve("subtest.txt"), "content");
        // Test
        var events = loader.eventStream().collect(Collectors.toList());

        // Assertions
        assertEquals(1, events.size());
        assertEquals("test.txt", events.get(0).getName());
    }

    @Test
    public void shouldHandleNonExistentDirectory() {
        loader = new DirectoryLoader(tempDir.resolve("nonexistent"));

        // Assertions
        assert(loader.eventStream().findAny().isEmpty());
    }
}
