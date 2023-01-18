package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration.migrations;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Principal;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.application.domain.models.TitleRating;
import com.lobox.assignments.imdb.application.services.TitleScoreService;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDatabase;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbProperties;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbSerializations;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys.*;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration.RocksDbMigration;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

@Component
public class InitialMigration implements RocksDbMigration {
    private final Logger logger = LoggerFactory.getLogger(InitialMigration.class);
    private final RocksDatabase rocks;
    private final RocksDbSerializations rocksDbSerializations;
    private final RocksDbProperties rocksDbProperties;
    private final TitleScoreService titleScoreService;
    private @Value("${application.datasets.title-ratings-tsv}") String ratingsTsv;
    private @Value("${application.datasets.title-basics-tsv}") String titleBasicsTsv;
    private @Value("${application.datasets.title-principals-tsv}") String titlePrincipalsTsv;
    private @Value("${application.datasets.title-crew-tsv}") String titleCrewTsv;
    private @Value("${application.datasets.name-basics-tsv}") String personsTsv;
    private @Value("${application.datasetBatchImportSize}") long batchImportSize;


    public InitialMigration(RocksDatabase rocks, RocksDbSerializations rocksDbSerializations, RocksDbProperties rocksDbProperties,
                            TitleScoreService titleScoreService) {

        this.rocks = rocks;
        this.rocksDbSerializations = rocksDbSerializations;
        this.rocksDbProperties = rocksDbProperties;
        this.titleScoreService = titleScoreService;
    }

    @Override
    public void migrate() {
        logger.info("Importing the datasets(will take a while)");
        importRatingsAndTitles();
        importPersons();
        importPrincipals();
        importCrews();
        logger.info("Finished importing the datasets");
    }

    private TreeSet<TitleRating> importRatings() {
        logger.info("Importing ratings...");
        final TreeSet<TitleRating> ratings = new TreeSet<>(Comparator.comparing(TitleRating::getTitleId));
        try (CsvReader csv = createCsvBuilder().build(Paths.get(ratingsTsv))) {
            csv.stream().skip(1).map(WrappedCsvRow::new).forEach(row -> {
                TitleRating rating = new TitleRating();
                rating.setTitleId(row.getField(0));
                rating.setAverageRating(Float.parseFloat(row.getField(1)));
                rating.setNumVotes(Integer.parseInt(row.getField(2)));
                ratings.add(rating);
            });
            ingestRatings(ratings);
            logger.info("Finished importing ratings");
            return ratings;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void importPrincipals() {
        logger.info("Importing principals...");
//        final TreeMap<String, byte[]> principalsBatch = new TreeMap<>();
        final TreeSet<PersonTitleCategoryKey> principalsSecondaryPersons = new TreeSet<>();
        try (CsvReader csv = createCsvBuilder().build(Paths.get(titlePrincipalsTsv))) {
            csv.stream().skip(1).map(WrappedCsvRow::new).forEach(csvRow -> {
                Principal principal = createPrincipalFromRow(csvRow);
                principalsSecondaryPersons.add(PersonTitleCategoryKey.fromPrincipal(principal));
                 /*principalsBatch.put(TitlePersonCategoryKey.fromPrincipal(principal).toString(), rocksDbSerializations.serializePrincipal(principal));
                if (principalsBatch.size() == batchImportSize) {
                    ingestPrincipals(principalsBatch);
                    logger.debug("Imported {} principals", batchImportSize);
                    principalsBatch.clear();
                }*/
                if (principalsSecondaryPersons.size() == batchImportSize) {
                    ingestPrincipalsSecondaryIndexPersons(principalsSecondaryPersons);
                    logger.debug("Imported {} principals", batchImportSize);
                    principalsSecondaryPersons.clear();
                }

            });
            /*if (principalsBatch.size() > 0) {
                ingestPrincipals(principalsBatch);
            }*/
            if (principalsSecondaryPersons.size() > 0) {
                ingestPrincipalsSecondaryIndexPersons(principalsSecondaryPersons);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished importing principals");
    }

    private static CsvReader.CsvReaderBuilder createCsvBuilder() {
        return CsvReader.builder()
                        .fieldSeparator('\t')
                        .quoteCharacter('\0')
                        .commentStrategy(CommentStrategy.SKIP)
                        .commentCharacter('#')
                        .skipEmptyRows(true)
                        .errorOnDifferentFieldCount(false);
    }


    private void importRatingsAndTitles() {
        TreeSet<TitleRating> ratings = importRatings();
        logger.info("Importing titles...");
        final Map<String, Float> scores = titleScoreService.calculateTitlesScore(ratings);
        final TreeSet<Title> titlesBatch = new TreeSet<>(Comparator.comparing(Title::getId));
        try (CsvReader csv = createCsvBuilder().build(Paths.get(titleBasicsTsv))) {
            csv.stream().skip(1).map(WrappedCsvRow::new).forEach(row -> {
                Title title = createTitleFromRow(row);
                title.setScore(scores.get(title.getId()));
                titlesBatch.add(title);
                if (titlesBatch.size() == batchImportSize) {
                    ingestTitles(titlesBatch);
                    logger.debug("Imported {} titles", batchImportSize);
                    titlesBatch.clear();
                }
            });
            if (titlesBatch.size() > 0) {
                ingestTitles(titlesBatch);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished importing titles");
    }

    private void importPersons() {
        logger.info("Importing persons...");
        TreeMap<String, Integer> deathYearsBatch = new TreeMap<>();
        final TreeSet<Person> personsBatch = new TreeSet<>(Comparator.comparing(Person::getId));
        try (CsvReader csv = createCsvBuilder().build(Paths.get(personsTsv))) {
            csv.stream().skip(1).map(WrappedCsvRow::new).forEach(row -> {
                Person person = createPersonFromRow(row);
                personsBatch.add(person);
                if (person.getDeathYear() != null) {
                    deathYearsBatch.put(String.format("%s.deathYear", person.getId()), person.getDeathYear());
                }
                if (personsBatch.size() == batchImportSize) {
                    ingestPersons(personsBatch);
                    logger.debug("Imported {} persons", batchImportSize);
                    personsBatch.clear();
                }
                if (deathYearsBatch.size() == batchImportSize) {
                    ingestPersonsDeathYear(deathYearsBatch);
                    deathYearsBatch.clear();
                }
            });
            if (personsBatch.size() > 0) {
                ingestPersons(personsBatch);
            }
            if (deathYearsBatch.size() > 0) {
                ingestPersonsDeathYear(deathYearsBatch);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Person createPersonFromRow(WrappedCsvRow row) {
        Person person = new Person();
        person.setId(row.getField(0));
        person.setPrimaryName(row.getField(1));
        if (row.getField(2) != null)
            person.setBirthYear(Integer.parseInt(row.getField(2)));
        if (row.getField(3) != null)
            person.setDeathYear(Integer.parseInt(row.getField(3)));
        if (row.getField(4) != null)
            person.setPrimaryProfession(Arrays.stream(row.getField(4).split(",")).toList());
        if (row.getField(5) != null)
            person.setKnownForTitles(Arrays.stream(row.getField(5).split(",")).toList());
        return person;
    }

    private Title createTitleFromRow(WrappedCsvRow row) {
        Title title = new Title();
        title.setId(row.getField(0));
        title.setTitleType(row.getField(1));
        title.setPrimaryTitle(row.getField(2));
        title.setOriginalTitle(row.getField(3));

        int adult = Integer.parseInt(row.getField(4));
        title.setAdult(adult == 1);
        if (row.getField(5) != null)
            title.setStartYear(Integer.parseInt(row.getField(5)));
        if (row.getField(6) != null)
            title.setEndYear(Integer.parseInt(row.getField(6)));
        if (row.getField(7) != null)
            title.setRuntimeMinutes(Integer.parseInt(row.getField(7)));
        if (row.getField(8) != null) {
            title.setGenres(Arrays.stream(row.getField(8).split(",")).toList());
        }
        title.setDirectors(Collections.emptyList());
        title.setWriters(Collections.emptyList());
        return title;
    }

    private Principal createPrincipalFromRow(WrappedCsvRow row) {
        Principal p = new Principal();
        p.setTitleId(row.getField(0));
        p.setPersonId(row.getField(2));
        p.setCategory(row.getField(3));
        p.setJob(row.getField(4));
        p.setCharacters(row.getField(5));
        return p;
    }

    private void importCrews() {
        logger.info("Importing crews...");
        TreeSet<TitleWritersKey> writers = new TreeSet<>();
        TreeSet<TitleDirectorsKey> directors = new TreeSet<>();
        try (CsvReader csv = createCsvBuilder().build(Paths.get(titleCrewTsv))) {
            csv.stream().skip(1).map(WrappedCsvRow::new).forEach(row -> {
                String titleId = row.getField(0);
                if (row.getField(1) != null) {
                    List<String> directorIds = Arrays.stream(row.getField(1).split(",")).map(String::trim).sorted().toList();
                    directors.add(new TitleDirectorsKey(titleId, directorIds));
                }
                if (row.getField(2) != null) {
                    List<String> writerIds = Arrays.stream(row.getField(2).split(",")).map(String::trim).sorted().toList();
                    writers.add(new TitleWritersKey(titleId, writerIds));
                }
                if (writers.size() == batchImportSize) {
                    ingestWriters(writers);
                    logger.debug("Imported {} writers", batchImportSize);
                    writers.clear();
                }
                if (directors.size() == batchImportSize) {
                    ingestDirectors(directors);
                    logger.debug("Imported {} directors", batchImportSize);
                    directors.clear();
                }
            });
            if (writers.size() > 0) {
                ingestWriters(writers);
            }
            if (directors.size() > 0) {
                ingestDirectors(directors);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Finished importing crews");
    }

    private void ingestPrincipals(TreeMap<String, byte[]> principals) {
        ingest(rocks.principalsPrimaryIndex(), sstFileWriter -> {
            try {
                for (Map.Entry<String, byte[]> entry : principals.entrySet()) {
                    sstFileWriter.put(entry.getKey().getBytes(), entry.getValue());
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }

        });
    }

    private void ingestPrincipalsSecondaryIndexPersons(TreeSet<PersonTitleCategoryKey> keys) {
        ingest(rocks.principalsSecondaryIndexPersons(), sstFileWriter -> {
            try {
                for (PersonTitleCategoryKey key : keys) {
                    sstFileWriter.put(key.toBytes(), new byte[0]);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }

        });
    }

    private void ingestTitles(Iterable<Title> titles) {
        final TreeSet<GenreYearScoreTitleKey> genreYearScoreKeys = new TreeSet<>();
        ingest(rocks.titlesPrimaryIndex(), sstFileWriter -> {
            try {
                for (Title title : titles) {
                    sstFileWriter.put(PrimaryKey.fromString(title.getId()).toBytes(), rocksDbSerializations.serializeTitle(title));
                    genreYearScoreKeys.addAll(GenreYearScoreTitleKey.fromTitle(title));

                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });

        ingest(rocks.titlesSecondaryIndexGenreStartYear(), sstFileWriter -> {
            try {
                for (GenreYearScoreTitleKey key : genreYearScoreKeys) {
                    sstFileWriter.put(key.toBytes(), new byte[0]);
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
                    sstFileWriter.put(PrimaryKey.fromString(person.getId()).toBytes(), rocksDbSerializations.serializePerson(person));
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
                    sstFileWriter.put(PrimaryKey.fromString(entry.getKey()).toBytes(),
                            ByteBuffer.allocate(4).putInt(entry.getValue()).array());
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestWriters(Iterable<TitleWritersKey> keys) {
        ingest(rocks.titlesSecondaryIndexWriters(), sstFileWriter -> {
            try {
                for (TitleWritersKey key : keys) {
                    sstFileWriter.put(key.toBytes(), new byte[0]);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestDirectors(Iterable<TitleDirectorsKey> keys) {
        ingest(rocks.titlesSecondaryIndexDirectors(), sstFileWriter -> {
            try {
                for (TitleDirectorsKey key : keys) {
                    sstFileWriter.put(key.toBytes(), new byte[0]);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void ingestRatings(Iterable<TitleRating> ratings) {
        ingest(rocks.ratingsPrimaryIndex(), sstFileWriter -> {
            try {
                for (TitleRating titleRating : ratings) {
                    sstFileWriter.put(PrimaryKey.fromString(titleRating.getTitleId()).toBytes(),
                            rocksDbSerializations.serializeTitleRating(titleRating));
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
        logger.debug("Ingesting data");
        EnvOptions envOptions = new EnvOptions();

        Options options = rocks.createOptions();
        try (SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);) {
            Files.createDirectories(Paths.get(rocksDbProperties.getDataDir(), "tmp"));
            Path sstFilePath = Files.createTempFile(Paths.get(rocksDbProperties.getDataDir(), "tmp"), null, ".sst");
            sstFileWriter.open(sstFilePath.toString());
            logger.debug("Writing to sst");
            writer.accept(sstFileWriter);
            sstFileWriter.finish();
            logger.debug("Finished writing to sst");
            // ingest
            ingestToIndex(index, sstFilePath);
            logger.debug("Finished ingesting data");
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override

    public int version() {
        return 2;
    }

    private record WrappedCsvRow(CsvRow row) {

        public String getField(int index) {
            String value = row.getField(index);
            if (value.equals("\\N"))
                return null;
            return value;
        }

    }
}
