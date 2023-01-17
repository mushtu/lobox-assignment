package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.repositories.PersonRepository;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Repository
public class RocksDbPersonRepository implements PersonRepository {

    private final RocksDatabase rocks;
    private final RocksDbSerializations rocksDbSerializations;

    public RocksDbPersonRepository(RocksDatabase rocks, RocksDbSerializations rocksDbSerializations) {
        this.rocks = rocks;
        this.rocksDbSerializations = rocksDbSerializations;
    }

    @Override
    public Collection<Person> findAllWithIdsAndAlive(Iterable<String> ids) {
        try {
            List<Person> alivePersons = new ArrayList<>();
            List<Person> persons = rocks.db().multiGetAsList(
                            StreamSupport.stream(ids.spliterator(), false).map(s -> rocks.personsPrimaryIndex()).toList(),
                            StreamSupport.stream(ids.spliterator(), false).map(String::getBytes).collect(Collectors.toList()))
                    .stream().map(rocksDbSerializations::deserializePerson).toList();
            for (Person p : persons) {
                if (rocks.db().get(rocks.personsSecondaryIndexDeathYear(), (p.getId() + ".deathYear").getBytes()) == null) {
                    alivePersons.add(p);
                }
            }
            return alivePersons;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

   /* public void insertMultiple(Iterable<Person> persons) {
        try (WriteBatch wb = new WriteBatch()) {
            for (Person person : persons) {
                wb.put(rocks.personsPrimaryIndex(), person.getId().getBytes(), serializePerson(person));
                if (person.getDeathYear() != null) {
                    String indexKey = person.getId() + ".deathYear";
                    wb.put(rocks.personsSecondaryIndexDeathYear(), indexKey.getBytes(), ByteBuffer.allocate(4).putInt(person.getDeathYear()).array());
                }
            }
            rocks.db().write(new WriteOptions(), wb);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }*/
}
