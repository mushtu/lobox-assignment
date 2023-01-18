package com.lobox.assignments.imdb.integration.configuration;


import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;

public class DbTempDataDirectory {
    private final RocksDbProperties properties;
    private final Path path;

    public DbTempDataDirectory(RocksDbProperties properties) {
        this.properties = properties;
        path = Paths.get(properties.getDataDir(), UUID.randomUUID().toString());
    }

    public Path create() {
        try {
            Files.createDirectories(path);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete() {
        try {
            Files.walk(path)
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
