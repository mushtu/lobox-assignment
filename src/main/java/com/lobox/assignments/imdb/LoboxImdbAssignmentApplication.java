package com.lobox.assignments.imdb;

import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties({RocksDbProperties.class})
@SpringBootApplication
public class LoboxImdbAssignmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoboxImdbAssignmentApplication.class, args);
    }

}
