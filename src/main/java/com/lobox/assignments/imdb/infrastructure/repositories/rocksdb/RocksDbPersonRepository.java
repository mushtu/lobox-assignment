package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.repositories.PersonRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;

@Repository
public class RocksDbPersonRepository implements PersonRepository {
    @Override
    public Collection<Person> FindAllWithIdsAndAlive(Iterable<String> ids) {
        return null;
    }
}
