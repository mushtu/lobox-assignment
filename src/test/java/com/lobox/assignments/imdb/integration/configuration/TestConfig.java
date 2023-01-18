package com.lobox.assignments.imdb.integration.configuration;

import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDatabase;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestConfig {

    @Bean()
    @Primary
    public RocksDatabase rocksDatabase(RocksDbProperties properties, DbTempDataDirectory tempDataDirectory) {
        properties.setDataDir(tempDataDirectory.create().toString());
        return new RocksDatabase(properties);
    }

    @Bean(destroyMethod = "delete")
    public DbTempDataDirectory dbTempDataDirectory(RocksDbProperties properties) {
        return new DbTempDataDirectory(properties);
    }
}
