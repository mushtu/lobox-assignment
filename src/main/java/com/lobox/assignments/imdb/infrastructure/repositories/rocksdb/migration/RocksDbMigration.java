package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration;

public interface RocksDbMigration {
    void migrate();

    int version();
}
