package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Repository
public class RocksDbTitleRepository implements TitleRepository {
    private final Logger logger = LoggerFactory.getLogger(RocksDbTitleRepository.class);
    private static final String TITLES_WRITER_INDEX = "titles-writer-index";
    private static final String TITLES_DIRECTOR_INDEX = "titles-director-index";
    private final RocksDbProperties rocksDbProperties;
    private RocksDB titlesDb;
    private ColumnFamilyHandle directorIndex;
    private ColumnFamilyHandle writerIndex;

    public RocksDbTitleRepository(RocksDbProperties rocksDbProperties) {
        this.rocksDbProperties = rocksDbProperties;
    }


    @PostConstruct
    void initialize() {
        RocksDB.loadLibrary();
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing(rocksDbProperties.isCreateIfMissing());
        options.setCreateMissingColumnFamilies(true);
        try {
            File titlesDbPath = new File(rocksDbProperties.getDataDir(), "titles");
            Files.createDirectories(titlesDbPath.getParentFile().toPath());
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            this.titlesDb = RocksDB.open(options, titlesDbPath.getAbsolutePath(),
                    List.of(new ColumnFamilyDescriptor("default".getBytes()), new ColumnFamilyDescriptor(TITLES_WRITER_INDEX.getBytes()),
                            new ColumnFamilyDescriptor(TITLES_DIRECTOR_INDEX.getBytes())), columnFamilyHandles);
            writerIndex = columnFamilyHandles.get(1);
            directorIndex = columnFamilyHandles.get(2);
        } catch (IOException | RocksDBException ex) {
            throw new RuntimeException("Error initializing RocksDB, check configurations and permissions.", ex);
        }
    }

    @PreDestroy
    void close() {
        if (this.titlesDb != null) {
            titlesDb.close();
        }
    }

    @Override
    public Optional<Title> FindById(String id) {
        try {
            byte[] bytes = titlesDb.get(id.getBytes());
            if (bytes != null) {
                return Optional.of(deserializeTitle(bytes));
            }
            return Optional.empty();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Title> FindAllByIds(Iterable<String> ids) {
        try {
            List<byte[]> values = titlesDb.multiGetAsList(StreamSupport.stream(ids.spliterator(), false).map(String::getBytes).collect(Collectors.toList()));
            return values.stream().map(this::deserializeTitle).collect(Collectors.toList());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Title> FindAllWithEqualDirectorAndWriter() {
        BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
        List<String> titleIds = new ArrayList<>();
        RocksIterator directorIterator = titlesDb.newIterator(directorIndex);
        RocksIterator writerIterator = titlesDb.newIterator(writerIndex);
        directorIterator.seekToFirst();
        writerIterator.seekToFirst();
        while (directorIterator.isValid() && writerIterator.isValid()) {
            byte[] directorKey = directorIterator.key();
            byte[] writerKey = writerIterator.key();
            int compareResult = comparator.compare(ByteBuffer.wrap(directorKey), ByteBuffer.wrap(writerKey));
            if (compareResult == 0) {
                String key = new String(directorKey);
                String[] tokens = key.split("\\.");
                titleIds.add(tokens[0]);
                directorIterator.next();
                writerIterator.next();
            } else {
                if (compareResult < 0) {
                    directorIterator.seek(writerKey);
                } else {
                    writerIterator.seek(directorKey);
                }
            }
        }
        return FindAllByIds(titleIds);
    }

    @Override
    public Title Save(Title title) {
        try {
            titlesDb.put(title.getId().getBytes(), serializeTitle(title));
            RocksIterator directorIterator = titlesDb.newIterator(directorIndex);
            RocksIterator writerIterator = titlesDb.newIterator(writerIndex);
            directorIterator.seek(title.getId().getBytes());
            writerIterator.seek(title.getId().getBytes());
            updateIndex(writerIndex, title.getId(), title.getWriters());
            updateIndex(directorIndex, title.getId(), title.getDirectors());
            return title;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateIndex(ColumnFamilyHandle indexFamily, String titleId, Iterable<String> subKeys) {
        try {
            RocksIterator iterator = titlesDb.newIterator(indexFamily);
            List<byte[]> previousKeys = new ArrayList<>();
            String indexPrefix = titleId + ".";
            for (iterator.seek(indexPrefix.getBytes()); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                if (!key.startsWith(indexPrefix))
                    break;
                previousKeys.add(iterator.key());
            }

            try (WriteBatch wb = new WriteBatch()) {
                for (byte[] oldKey : previousKeys) {
                    wb.delete(indexFamily, oldKey);
                }
                for (String subKey : subKeys) {
                    String indexKey = titleId + "." + subKey;
                    wb.put(indexFamily, indexKey.getBytes(), new byte[0]);
                }
                titlesDb.write(new WriteOptions(), wb);
            }

        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }

    private byte[] serializeTitle(Title title) {
        return SerializationUtils.serialize(title);
    }

    private Title deserializeTitle(byte[] value) {
        return (Title) SerializationUtils.deserialize(value);
    }
}
