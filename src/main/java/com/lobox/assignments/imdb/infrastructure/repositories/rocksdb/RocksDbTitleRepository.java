package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.*;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys.*;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Repository
public class RocksDbTitleRepository implements TitleRepository {
    private final Logger logger = LoggerFactory.getLogger(RocksDbTitleRepository.class);
    private final RocksDatabase rocks;
    private final RocksDbSerializations rocksDbSerializations;

    public RocksDbTitleRepository(RocksDatabase rocks, RocksDbSerializations rocksDbSerializations) {
        this.rocks = rocks;
        this.rocksDbSerializations = rocksDbSerializations;
    }


    private void setTitleCrews(Title title) {
        title.setDirectors(getTitleDirectorsId(title.getId()));
        title.setWriters(getTitleWritersId(title.getId()));
    }

    private Collection<String> getTitleDirectorsId(String titleId) {
        try (RocksIterator itr = rocks.db().newIterator(rocks.titlesSecondaryIndexDirectors())) {
            return TitleDirectorsKey.titleDirectors(itr, titleId);
        }
    }

    private Collection<String> getTitleWritersId(String titleId) {
        try (RocksIterator itr = rocks.db().newIterator(rocks.titlesSecondaryIndexWriters())) {
            return TitleWritersKey.titleWriters(itr, titleId);
        }
    }

    @Override
    public Optional<Title> findById(String id) {
        try {
            byte[] bytes = rocks.db().get(rocks.titlesPrimaryIndex(), PrimaryKey.fromString(id).toBytes());
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
        if (ids == null)
            return Collections.emptyList();
        List<byte[]> keys = StreamSupport.stream(ids.spliterator(), false).map(s -> PrimaryKey.fromString(s).toBytes()).toList();
        if (keys.size() == 0)
            return Collections.emptyList();
        try {
            List<byte[]> values = rocks.db().multiGetAsList(StreamSupport.stream(ids.spliterator(), false).map(s -> rocks.titlesPrimaryIndex()).toList(), keys);
            List<Title> titles = values.stream().filter(Objects::nonNull).map(rocksDbSerializations::deserializeTitle).toList();
            titles.forEach(this::setTitleCrews);
            return titles;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Title> findAllWithEqualDirectorAndWriterAndAlive(PageRequest<String> pageRequest) {
        List<String> titleIds = new ArrayList<>();
        try (RocksIterator directorIterator = rocks.db().newIterator(rocks.titlesSecondaryIndexDirectors());
             RocksIterator writerIterator = rocks.db().newIterator(rocks.titlesSecondaryIndexWriters())
        ) {
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
                TitleDirectorsKey directorKey = TitleDirectorsKey.fromBytes(directorIterator.key());
                TitleWritersKey writerKey = TitleWritersKey.fromBytes(writerIterator.key());
                int compareResult = directorKey.compareTo(writerKey);
                if (compareResult == 0) {
                    Collection<String> persons = directorKey.getDirectors();
                    if (findAllPersonsWithIdsAndAlive(persons).size() == persons.size()) {
                        titleIds.add(directorKey.getTitleId());
                    }
                    directorIterator.next();
                    writerIterator.next();
                } else {
                    if (compareResult < 0) {
                        directorIterator.seek(writerKey.toBytes());
                    } else {
                        writerIterator.seek(directorKey.toBytes());
                    }
                }
            }
            return findAllByIds(titleIds);
        }
    }

    @Override
    public Iterable<Title> findActorsCommonTitles(String firstActorId, String secondActorId) {
        List<String> commonTitles = new ArrayList<>();
        try (RocksIterator firstItr = rocks.db().newIterator(rocks.principalsSecondaryIndexPersons());
             RocksIterator secondItr = rocks.db().newIterator(rocks.principalsSecondaryIndexPersons())) {
            Collection<PersonTitleCategoryKey> firstActorKeys = PersonTitleCategoryKey
                    .keysStartWithPersonEndWithCategory(firstItr, firstActorId, PrincipalCategory.ACTOR);
            Collection<PersonTitleCategoryKey> secondActorKeys = PersonTitleCategoryKey
                    .keysStartWithPersonEndWithCategory(secondItr, secondActorId, PrincipalCategory.ACTOR);
            firstActorKeys.stream()
                          .map(PersonTitleCategoryKey::getTitleId)
                          .filter(titleId -> secondActorKeys.stream().anyMatch(s -> s.getTitleId().equals(titleId)))
                          .forEach(commonTitles::add);
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

    @Override
    public Map<Integer, Title> findBestTitleOnEachYearByGenre(String genre) {
        Map<Integer, Title> result = new HashMap<>();
        try (RocksIterator genreItr = rocks.db().newIterator(rocks.titlesSecondaryIndexGenreStartYear())) {
            genreItr.seekForPrev(GenreYearScoreTitleKey.withPrefix(genre, Integer.MAX_VALUE));
            while (genreItr.isValid()) {
                GenreYearScoreTitleKey key = GenreYearScoreTitleKey.fromBytes(genreItr.key());
                if (!key.getGenre().equals(genre))
                    break;
                Title title = findById(key.getTitleId()).get();
                title.setScore(key.getScore());
                result.put(key.getYear(), title);
                // go to the previous year
                genreItr.seekForPrev(GenreYearScoreTitleKey.withPrefix(genre, key.getYear()));
            }
        }
        return result;
    }

    private Collection<Person> findAllPersonsWithIdsAndAlive(Iterable<String> ids) {
        try {
            List<Person> alivePersons = new ArrayList<>();
            List<Person> persons =
                    rocks.db()
                         .multiGetAsList(
                                 StreamSupport.stream(ids.spliterator(), false).map(s -> rocks.personsPrimaryIndex()).toList(),
                                 StreamSupport.stream(ids.spliterator(), false).map(s -> PrimaryKey.fromString(s).toBytes()).collect(Collectors.toList()))
                         .stream().filter(Objects::nonNull).map(rocksDbSerializations::deserializePerson).toList();
            for (Person p : persons) {
                if (rocks.db().get(rocks.personsSecondaryIndexDeathYear(), PrimaryKey.fromString(p.getId()).toBytes()) == null) {
                    alivePersons.add(p);
                }
            }
            return alivePersons;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
