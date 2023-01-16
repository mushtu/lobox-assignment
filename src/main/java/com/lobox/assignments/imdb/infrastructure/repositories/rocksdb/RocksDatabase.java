package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@Component
public class RocksDatabase {
    private final Logger logger = LoggerFactory.getLogger(RocksDbTitleRepository.class);
    private static final String DATABASE_NAME = "imdb";

    private static final String TITLES_PRIMARY_INDEX_NAME = "titles";
    private static final String PERSONS_PRIMARY_INDEX_NAME = "persons";
    private static final String TITLES_WRITER_INDEX_NAME = "titles-writer-index";
    private static final String TITLES_DIRECTOR_INDEX_NAME = "titles-director-index";
    private static final String PERSONS_DEATH_YEAR_INDEX_NAME = "persons-death-year-index";
    private static final String DEFAULT_INDEX_NAME = "default";
    private final RocksDbProperties rocksDbProperties;
    private RocksDB db;
    private ColumnFamilyHandle titlesPrimaryIndex;
    private ColumnFamilyHandle personsPrimaryIndex;
    private ColumnFamilyHandle titlesSecondaryIndexWriters;
    private ColumnFamilyHandle titlesSecondaryIndexDirectors;
    private ColumnFamilyHandle personsSecondaryIndexDeathYear;

    public RocksDatabase(RocksDbProperties rocksDbProperties) {
        this.rocksDbProperties = rocksDbProperties;
    }

    @PostConstruct
    void initialize() {
        RocksDB.loadLibrary();
        DBOptions options = createDbOptions();

        try {
            File dbPath = new File(rocksDbProperties.getDataDir(), DATABASE_NAME);
            Files.createDirectories(dbPath.getParentFile().toPath());
            ColumnFamilyOptions columnFamilyOptions = createColumnFamilyOptions();
            List<ColumnFamilyDescriptor> columnFamilies = List.of(
                    new ColumnFamilyDescriptor(DEFAULT_INDEX_NAME.getBytes(), columnFamilyOptions),
                    new ColumnFamilyDescriptor(TITLES_PRIMARY_INDEX_NAME.getBytes(), columnFamilyOptions),
                    new ColumnFamilyDescriptor(TITLES_WRITER_INDEX_NAME.getBytes(), columnFamilyOptions),
                    new ColumnFamilyDescriptor(TITLES_DIRECTOR_INDEX_NAME.getBytes(), columnFamilyOptions),
                    new ColumnFamilyDescriptor(PERSONS_PRIMARY_INDEX_NAME.getBytes(), columnFamilyOptions),
                    new ColumnFamilyDescriptor(PERSONS_DEATH_YEAR_INDEX_NAME.getBytes(), columnFamilyOptions)
            );
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            this.db = RocksDB.open(options, dbPath.getAbsolutePath(), columnFamilies, columnFamilyHandles);
            titlesPrimaryIndex = columnFamilyHandles.get(1);
            titlesSecondaryIndexWriters = columnFamilyHandles.get(2);
            titlesSecondaryIndexDirectors = columnFamilyHandles.get(3);
            personsPrimaryIndex = columnFamilyHandles.get(4);
            personsSecondaryIndexDeathYear = columnFamilyHandles.get(5);
        } catch (IOException | RocksDBException ex) {
            throw new RuntimeException("Error initializing RocksDB, check configurations and permissions.", ex);
        }
    }


    @PreDestroy
    void close() {
        if (this.db != null) {
            db.close();
        }
    }

    public RocksDB db() {
        return db;
    }

    public ColumnFamilyHandle titlesPrimaryIndex() {
        return titlesPrimaryIndex;
    }

    public ColumnFamilyHandle personsPrimaryIndex() {
        return personsPrimaryIndex;
    }

    public ColumnFamilyHandle titlesSecondaryIndexWriters() {
        return titlesSecondaryIndexWriters;
    }

    public ColumnFamilyHandle titlesSecondaryIndexDirectors() {
        return titlesSecondaryIndexDirectors;
    }

    public DBOptions createDbOptions() {
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(rocksDbProperties.isCreateIfMissing());
        dbOptions.setCreateMissingColumnFamilies(rocksDbProperties.isCreateMissingColumnFamilies());
        dbOptions.setMaxOpenFiles(rocksDbProperties.getMaxOpenFiles());
        dbOptions.setMaxBackgroundJobs(8);
        return dbOptions;
    }

    public ColumnFamilyOptions createColumnFamilyOptions() {
        ColumnFamilyOptions options = new ColumnFamilyOptions();
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockSize(rocksDbProperties.getBlockSize());
        // Set memory table size
        options.setMaxWriteBufferNumber(rocksDbProperties.getMaxWriteBufferNumber());
        options.setWriteBufferSize(rocksDbProperties.getWriteBufferSize());
        options.setMinWriteBufferNumberToMerge(rocksDbProperties.getMinWriteBufferNumberToMerge());
        // Set block cache size
        tableOptions.setBlockCache(new LRUCache(rocksDbProperties.getBlockCacheSize()));
        // Put all index into block cache
        tableOptions.setCacheIndexAndFilterBlocks(rocksDbProperties.isCacheIndexAndFilterBlocks());

        options.setTableFormatConfig(tableOptions);

        options.setTargetFileSizeBase(rocksDbProperties.getTargetFileSizeBase());
        options.setCompactionStyle(rocksDbProperties.getCompactionStyle());
        options.setLevelZeroFileNumCompactionTrigger(rocksDbProperties.getLevelZeroFileNumCompactionTrigger());
        options.setLevelZeroSlowdownWritesTrigger(rocksDbProperties.getLevelZeroSlowdownWritesTrigger());
        options.setLevelZeroStopWritesTrigger(rocksDbProperties.getLevelZeroStopWritesTrigger());
        options.setNumLevels(rocksDbProperties.getNumLevels());
        options.setMaxBytesForLevelBase(rocksDbProperties.getMaxBytesForLevelBase());
        return options;
    }

    public Options createOptions() {
        Options options = new Options();
        options.setCreateIfMissing(rocksDbProperties.isCreateIfMissing());
        options.setCreateMissingColumnFamilies(rocksDbProperties.isCreateMissingColumnFamilies());
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockSize(rocksDbProperties.getBlockSize());
        options.setMaxBackgroundJobs(8);
        // Set memory table size
        options.setMaxWriteBufferNumber(rocksDbProperties.getMaxWriteBufferNumber());
        options.setWriteBufferSize(rocksDbProperties.getWriteBufferSize());

        // Set block cache size
        tableOptions.setBlockCache(new LRUCache(rocksDbProperties.getBlockCacheSize()));
        // Put all index into block cache
        tableOptions.setCacheIndexAndFilterBlocks(rocksDbProperties.isCacheIndexAndFilterBlocks());

        options.setTableFormatConfig(tableOptions);

        options.setTargetFileSizeBase(rocksDbProperties.getTargetFileSizeBase());
        options.setMaxOpenFiles(rocksDbProperties.getMaxOpenFiles());
        options.setCompactionStyle(rocksDbProperties.getCompactionStyle());
        options.setLevelZeroFileNumCompactionTrigger(rocksDbProperties.getLevelZeroFileNumCompactionTrigger());
        options.setLevelZeroSlowdownWritesTrigger(rocksDbProperties.getLevelZeroSlowdownWritesTrigger());
        options.setLevelZeroStopWritesTrigger(rocksDbProperties.getLevelZeroStopWritesTrigger());
        options.setNumLevels(rocksDbProperties.getNumLevels());
        options.setMaxBytesForLevelBase(rocksDbProperties.getMaxBytesForLevelBase());
        return options;
    }

    public ColumnFamilyHandle personsSecondaryIndexDeathYear() {
        return personsSecondaryIndexDeathYear;
    }
}
