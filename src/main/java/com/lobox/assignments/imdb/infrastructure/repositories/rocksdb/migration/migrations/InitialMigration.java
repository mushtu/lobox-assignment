package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration.migrations;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDatabase;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbSerializations;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration.RocksDbMigration;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration.StringArrayConversion;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

@Component
public class InitialMigration implements RocksDbMigration {
    private final Logger logger = LoggerFactory.getLogger(InitialMigration.class);
    private static final int BATCH_INSERT_SIZE = 100000;
    private final RocksDatabase rocks;
    private final RocksDbSerializations rocksDbSerializations;
    private @Value("${application.datasets.title-basics-tsv}") String titleBasicsTsv;
    private @Value("${application.datasets.title-crew-tsv}") String titleCrewTsv;
    private @Value("${application.datasets.name-basics-tsv}") String personsTsv;
    private @Value("${application.datasetReaderBufferSize}") int datasetReaderBufferSize;

    public InitialMigration(RocksDatabase rocks, RocksDbSerializations rocksDbSerializations) {

        this.rocks = rocks;
        this.rocksDbSerializations = rocksDbSerializations;
    }

    @Override
    public void migrate() {
        logger.info("Importing the datasets(will take a while)");
        importTitles();
        importCrews();
        importPersons();
        logger.info("Finished importing the datasets");
    }

    private void importTitles() {
        logger.info("Importing titles...");
        TsvParserSettings settings = createTsvParserSettings();
        final TreeSet<Title> titlesBatch = new TreeSet<>(Comparator.comparing(Title::getId));
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] row, ParsingContext context) {
                Title title = createTitleFromRow(row);
                titlesBatch.add(title);
                if (titlesBatch.size() == BATCH_INSERT_SIZE) {
                    ingestTitles(titlesBatch);
                    logger.debug("Imported {} titles", BATCH_INSERT_SIZE);
                    titlesBatch.clear();
                }
            }
        };

        rowProcessor.convertAll(Conversions.toNull("\\N"));
        rowProcessor.convertFields(Conversions.string()).set("tconst", "primaryTitle", "originalTitle");
        rowProcessor.convertFields(Conversions.toInteger()).set("isAdult");
        rowProcessor.convertFields(Conversions.toInteger()).set("startYear", "endYear", "runtimeMinutes");
        rowProcessor.convertFields(Conversions.string(), new StringArrayConversion()).set("genres");
        settings.setProcessor(rowProcessor);
        TsvParser parser = new TsvParser(settings);
        try {
            parser.parse(new FileReader(titleBasicsTsv));
            if (titlesBatch.size() > 0) {
                ingestTitles(titlesBatch);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished importing titles");
    }

    private void importPersons() {
        logger.info("Importing persons...");
        TreeMap<String, Integer> deathYearsBatch = new TreeMap<>();
        TsvParserSettings settings = createTsvParserSettings();
        final TreeSet<Person> personsBatch = new TreeSet<>(Comparator.comparing(Person::getId));
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] row, ParsingContext context) {
                Person person = createPersonFromRow(row);
                personsBatch.add(person);
                if (person.getDeathYear() != null) {
                    deathYearsBatch.put(String.format("%s.deathYear", person.getId()), person.getDeathYear());
                }
                if (personsBatch.size() == BATCH_INSERT_SIZE) {
                    ingestPersons(personsBatch);
                    logger.debug("Imported {} persons", BATCH_INSERT_SIZE);
                    personsBatch.clear();
                }
                if (deathYearsBatch.size() == BATCH_INSERT_SIZE) {
                    ingestPersonsDeathYear(deathYearsBatch);
                    deathYearsBatch.clear();
                }
            }
        };

        rowProcessor.convertAll(Conversions.toNull("\\N"));
        rowProcessor.convertFields(Conversions.string()).set("nconst", "primaryName");
        rowProcessor.convertFields(Conversions.toInteger()).set("birthYear", "deathYear");
        rowProcessor.convertFields(Conversions.string(), new StringArrayConversion()).set("primaryProfession", "knownForTitles");
        settings.setProcessor(rowProcessor);
        TsvParser parser = new TsvParser(settings);
        try {
            parser.parse(new FileReader(personsTsv));
            if (personsBatch.size() > 0) {
                ingestPersons(personsBatch);
            }
            if (deathYearsBatch.size() > 0) {
                ingestPersonsDeathYear(deathYearsBatch);
            }
            logger.info("Finished importing persons");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Person createPersonFromRow(Object[] row) {
        Person person = new Person();
        person.setId(Objects.toString(row[0]));
        person.setPrimaryName(Objects.toString(row[1]));
        person.setBirthYear((Integer) row[2]);
        person.setDeathYear((Integer) row[3]);
        person.setPrimaryProfession((Collection<String>) row[4]);
        person.setKnownForTitles((Collection<String>) row[5]);
        return person;
    }

    private Title createTitleFromRow(Object[] row) {
        Title title = new Title();
        title.setId(Objects.toString(row[0]));
        title.setPrimaryTitle(Objects.toString(row[2]));
        title.setOriginalTitle(Objects.toString(row[3]));
        Integer adult = (Integer) row[4];
        title.setAdult(adult == 1);
        title.setStartYear((Integer) row[5]);
        title.setEndYear((Integer) row[6]);
        title.setRuntimeMinutes((Integer) row[7]);
        title.setGenres((Collection<String>) row[8]);
        title.setDirectors(Collections.emptyList());
        title.setWriters(Collections.emptyList());
        return title;
    }

    private void importCrews() {
        logger.info("Importing crews...");
        TreeSet<String> writers = new TreeSet<>();
        TreeSet<String> directors = new TreeSet<>();
        TsvParserSettings parserSettings = createTsvParserSettings();
        ObjectRowProcessor rowProcessor = new ObjectRowProcessor() {
            @Override
            public void rowProcessed(Object[] row, ParsingContext context) {
                String titleId = Objects.toString(row[0]);
                if (row[1] != null && row[1] instanceof Collection<?> titleDirectors && !titleDirectors.isEmpty()) {
                    String directorsSubKey = String.join(",", ((Collection<String>) row[1]).stream().sorted().toList());
                    directors.add(String.format("%s.%s", titleId, directorsSubKey));
                }
                if (row[2] != null && row[2] instanceof Collection<?> titleWriters && !titleWriters.isEmpty()) {
                    String writersSubKey = String.join(",", ((Collection<String>) row[2]).stream().sorted().toList());
                    writers.add(String.format("%s.%s", titleId, writersSubKey));
                }
                if (writers.size() == BATCH_INSERT_SIZE) {
                    ingestWriters(writers);
                    logger.debug("Imported {} writers", BATCH_INSERT_SIZE);
                    writers.clear();
                }
                if (directors.size() == BATCH_INSERT_SIZE) {
                    ingestDirectors(directors);
                    logger.debug("Imported {} directors", BATCH_INSERT_SIZE);
                    directors.clear();
                }
            }
        };

        rowProcessor.convertAll(Conversions.toNull("\\N"));
        rowProcessor.convertFields(Conversions.string()).set("tconst");
        rowProcessor.convertFields(Conversions.string(), new StringArrayConversion()).set("directors", "writers");
        parserSettings.setProcessor(rowProcessor);
        TsvParser parser = new TsvParser(parserSettings);
        try {
            parser.parse(new FileReader(titleCrewTsv));
            if (writers.size() > 0) {
                ingestWriters(writers);
            }
            if (directors.size() > 0) {
                ingestDirectors(directors);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished importing crews");
    }

    private TsvParserSettings createTsvParserSettings() {
        TsvParserSettings parserSettings = new TsvParserSettings();
        parserSettings.setMaxCharsPerColumn(15000);
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.setInputBufferSize(datasetReaderBufferSize);
        return parserSettings;
    }

    private void ingestTitles(Iterable<Title> titles) {
        ingest(rocks.titlesPrimaryIndex(), sstFileWriter -> {
            try {
                for (Title title : titles) {
                    sstFileWriter.put(title.getId().getBytes(), rocksDbSerializations.serializeTitle(title));
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }

        });
    }

    private void ingestPersons(Iterable<Person> persons) {
        ingest(rocks.personsPrimaryIndex(), sstFileWriter -> {
            try {
                for (Person person : persons) {
                    sstFileWriter.put(person.getId().getBytes(), rocksDbSerializations.serializePerson(person));
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }

        });
    }

    private void ingestPersonsDeathYear(TreeMap<String, Integer> deathYears) {
        ingest(rocks.personsSecondaryIndexDeathYear(), sstFileWriter -> {
            try {
                for (Map.Entry<String, Integer> entry : deathYears.entrySet()) {
                    sstFileWriter.put(entry.getKey().getBytes(), ByteBuffer.allocate(4).putInt(entry.getValue()).array());
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestWriters(Iterable<String> keys) {
        ingest(rocks.titlesSecondaryIndexWriters(), sstFileWriter -> {
            try {
                for (String key : keys) {
                    sstFileWriter.put(key.getBytes(), new byte[0]);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestDirectors(Iterable<String> keys) {
        ingest(rocks.titlesSecondaryIndexDirectors(), sstFileWriter -> {
            try {
                for (String key : keys) {
                    sstFileWriter.put(key.getBytes(), new byte[0]);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestToIndex(ColumnFamilyHandle index, Path sstFilePath) throws RocksDBException {
        try (IngestExternalFileOptions options = new IngestExternalFileOptions()) {
            options.setMoveFiles(true);
            rocks.db().ingestExternalFile(index, List.of(sstFilePath.toString()), new IngestExternalFileOptions());
        }
        try {
            Files.delete(sstFilePath);
        } catch (Exception ignored) {
        }
    }

    void ingest(ColumnFamilyHandle index, Consumer<SstFileWriter> writer) {
        try (SstFileWriter sstFileWriter = new SstFileWriter(new EnvOptions(), rocks.createOptions());) {
            Path sstFilePath = Files.createTempFile(null, "sst");
            sstFileWriter.open(sstFilePath.toString());
            writer.accept(sstFileWriter);
            sstFileWriter.finish();
            // ingest
            ingestToIndex(index, sstFilePath);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override

    public int version() {
        return 1;
    }

}
