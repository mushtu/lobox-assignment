package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.PageRequest;
import com.lobox.assignments.imdb.application.domain.models.PrincipalCategory;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.application.domain.models.TitleRating;
import com.lobox.assignments.imdb.application.domain.repositories.PersonRepository;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.util.BytewiseComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Repository
public class RocksDbTitleRepository implements TitleRepository {
    private final Logger logger = LoggerFactory.getLogger(RocksDbTitleRepository.class);
    private final RocksDatabase rocks;
    private final PersonRepository personRepository;
    private final RocksDbSerializations rocksDbSerializations;

    public RocksDbTitleRepository(RocksDatabase rocks, PersonRepository personRepository, RocksDbSerializations rocksDbSerializations) {
        this.rocks = rocks;
        this.personRepository = personRepository;
        this.rocksDbSerializations = rocksDbSerializations;
    }


    private void setTitleCrews(Title title) {
        title.setDirectors(getTitleDirectorsId(title.getId()));
        title.setWriters(getTitleWritersId(title.getId()));
    }

    private Collection<String> getTitleDirectorsId(String titleId) {
        try (RocksIterator itr = rocks.db().newIterator(rocks.titlesSecondaryIndexDirectors())) {
            itr.seek(titleId.getBytes());
            if (itr.isValid()) {
                String key = new String(itr.key());
                return Arrays.stream(key.split("\\.")[1].split(",")).toList();
            }
        }
        return Collections.emptyList();
    }

    private Collection<String> getTitleWritersId(String titleId) {
        try (RocksIterator itr = rocks.db().newIterator(rocks.titlesSecondaryIndexWriters())) {
            itr.seek(titleId.getBytes());
            if (itr.isValid()) {
                String key = new String(itr.key());
                return Arrays.stream(key.split("\\.")[1].split(",")).toList();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public Optional<Title> findById(String id) {
        try {
            byte[] bytes = rocks.db().get(rocks.titlesPrimaryIndex(), id.getBytes());
            if (bytes != null) {
                Title title = rocksDbSerializations.deserializeTitle(bytes);
                setTitleCrews(title);
                return Optional.of(title);
            }
            return Optional.empty();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Title> findAllByIds(Iterable<String> ids) {
        try {
            List<byte[]> values = rocks.db().multiGetAsList(StreamSupport.stream(ids.spliterator(), false).map(s -> rocks.titlesPrimaryIndex()).toList(),
                    StreamSupport.stream(ids.spliterator(), false).map(String::getBytes).collect(Collectors.toList()));
            List<Title> titles = values.stream().map(rocksDbSerializations::deserializeTitle).toList();
            titles.forEach(this::setTitleCrews);
            return titles;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Title> findAllWithEqualDirectorAndWriterAndAlive(PageRequest<String> pageRequest) {
        BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
        List<String> titleIds = new ArrayList<>();
        try (RocksIterator directorIterator = rocks.db().newIterator(rocks.titlesSecondaryIndexDirectors())) {
            try (RocksIterator writerIterator = rocks.db().newIterator(rocks.titlesSecondaryIndexWriters())) {
                if (pageRequest.getLastKey() != null) {
                    directorIterator.seek(pageRequest.getLastKey().getBytes());
                    writerIterator.seek(pageRequest.getLastKey().getBytes());
                    if (directorIterator.isValid() && writerIterator.isValid()) {
                        directorIterator.next();
                        writerIterator.next();
                    }
                } else {
                    directorIterator.seekToFirst();
                    writerIterator.seekToFirst();
                }
                while (directorIterator.isValid() && writerIterator.isValid() && titleIds.size() < pageRequest.getPageSize()) {
                    byte[] directorKey = directorIterator.key();
                    byte[] writerKey = writerIterator.key();
                    int compareResult = comparator.compare(ByteBuffer.wrap(directorKey), ByteBuffer.wrap(writerKey));
                    if (compareResult == 0) {
                        String key = new String(directorKey);
                        String[] tokens = key.split("\\.");
                        List<String> persons = Arrays.stream(tokens[1].split(",")).toList();
                        if (personRepository.findAllWithIdsAndAlive(persons).size() == persons.size()) {
                            titleIds.add(tokens[0]);
                        }
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
                return findAllByIds(titleIds);
            }
        }
    }

    @Override
    public Iterable<Title> findActorsCommonTitles(String firstActorId, String secondActorId) {
        // principals secondary index key format => {personId}.{titleId}.{category}
        List<String> commonTitles = new ArrayList<>();
        try (RocksIterator firstItr = rocks.db().newIterator(rocks.principalsSecondaryIndexPersons());
             RocksIterator secondItr = rocks.db().newIterator(rocks.principalsSecondaryIndexPersons())) {
            List<String> firstActorKeys = getIteratorKeysStartsWith(firstItr, firstActorId + ".").stream()
                                                                                                 .map(key -> key.substring(key.indexOf(".") + 1)).toList();
            List<String> secondActorKeys = getIteratorKeysStartsWith(secondItr, secondActorId + ".").stream()
                                                                                                    .map(key -> key.substring(key.indexOf(".") + 1)).toList();
            firstActorKeys.stream()
                          .filter(key -> key.endsWith(PrincipalCategory.ACTOR))
                          .filter(secondActorKeys::contains)
                          .map(key -> {
                              String[] tokens = key.split("\\.");
                              return tokens[0];
                          }).forEach(commonTitles::add);
        }
        return findAllByIds(commonTitles);
    }

    @Override
    public Collection<TitleRating> findAllRatings() {
        List<TitleRating> ratings = new ArrayList<>();
        try (RocksIterator itr = rocks.db().newIterator(rocks.ratingsPrimaryIndex())) {
            itr.seekToFirst();
            while (itr.isValid()) {
                TitleRating rating = rocksDbSerializations.deserializeTitleRating(itr.value());
                ratings.add(rating);
                itr.next();
            }
        }
        return ratings;
    }

    private Optional<Float> getTitleScore(String titleId) {
        try {
            byte[] value = rocks.db().get(rocks.titlesSecondaryIndexScore(), titleId.getBytes());
            if (value != null) return Optional.of(Float.parseFloat(new String(value)));
            return Optional.empty();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, Title> findBestTitleOnEachYearByGenre(String genre) {
        Map<Integer, Title> result = new HashMap<>();
        try (RocksIterator genreItr = rocks.db().newIterator(rocks.titlesSecondaryIndexGenreStartYear())) {
            genreItr.seekForPrev((genre + "|" + Integer.MAX_VALUE + "|").getBytes());
            while (genreItr.isValid()) {
                // key format => {genre}|{startYear}|{score}|{titleId}
                String key = new String(genreItr.key());
                if (!key.startsWith(genre + "|"))
                    break;
                String[] keyParts = key.split("\\|");
                Integer year = Integer.parseInt(keyParts[1]);
                String titleId = keyParts[3];
                float maxScore = Float.parseFloat(keyParts[2]);
                Title title = findById(titleId).get();
                title.setScore(maxScore);
                result.put(year, title);
                // go to the previous year
                genreItr.seekForPrev(String.format("%s|%s|", genre, year).getBytes());
            }
        }
        return result;
    }

    private List<String> getIteratorKeysStartsWith(RocksIterator itr, String prefix) {
        List<String> keys = new ArrayList<>();
        itr.seek(prefix.getBytes());
        while (itr.isValid()) {
            String key = new String(itr.key());
            if (!key.startsWith(prefix))
                break;
            keys.add(key);
            itr.next();
        }
        return keys;
    }

}
