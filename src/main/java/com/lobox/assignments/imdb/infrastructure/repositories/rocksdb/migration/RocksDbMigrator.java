package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration;

import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;
import jakarta.annotation.PostConstruct;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Optional;

@Component
@Profile("!test")
public class RocksDbMigrator {

    private static final String MIGRATION_DB = "migration-history";
    private final Logger logger = LoggerFactory.getLogger(RocksDbMigrator.class);
    private final RocksDbProperties rocksDbProperties;
    private final ObjectProvider<RocksDbMigration> migrations;

    private RocksDB migrationDb;

    public RocksDbMigrator(RocksDbProperties rocksDbProperties, ObjectProvider<RocksDbMigration> migrations) {
        this.rocksDbProperties = rocksDbProperties;
        this.migrations = migrations;
    }

    @PostConstruct
    void initialize() {
        logger.info("Migration started");
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            File migrationDbPath = new File(rocksDbProperties.getDataDir(), MIGRATION_DB);
            Files.createDirectories(migrationDbPath.getParentFile().toPath());
            this.migrationDb = RocksDB.open(options, migrationDbPath.getAbsolutePath());
            migrate();
            logger.info("Migration finished successfully");
        } catch (IOException | RocksDBException ex) {
            throw new RuntimeException("Error initializing RocksDB, check configurations and permissions.", ex);
        }
    }

    private void migrate() {
        Integer latestVersion = getAppliedVersion().orElse(Integer.MIN_VALUE);
        for (RocksDbMigration migration : migrations.stream().filter(m -> m.version() > latestVersion).sorted(Comparator.comparingInt(RocksDbMigration::version)).toList()) {
            migration.migrate();
            setAppliedVersion(migration.version());
        }

    }

    private Optional<Integer> getAppliedVersion() {
        try {
            byte[] bytes = migrationDb.get("version".getBytes());
            if (bytes != null) {
                return Optional.of(Integer.parseInt(new String(bytes)));
            }
            return Optional.empty();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private void setAppliedVersion(Integer version) {
        try {
            migrationDb.put("version".getBytes(), version.toString().getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

}
