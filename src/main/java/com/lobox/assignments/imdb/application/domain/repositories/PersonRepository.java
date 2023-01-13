package com.lobox.assignments.imdb.application.domain.repositories;

import com.lobox.assignments.imdb.application.domain.models.Person;

import java.util.Collection;

public interface PersonRepository {
    Collection<Person> FindAllWithIdsAndAlive(Iterable<String> ids);
}
