package com.lobox.assignments.imdb.application.domain.repositories;

import com.lobox.assignments.imdb.application.domain.models.PageRequest;
import com.lobox.assignments.imdb.application.domain.models.Title;

import java.util.Collection;
import java.util.Optional;

public interface TitleRepository {
    Optional<Title> FindById(String id);

    Collection<Title> FindAllByIds(Iterable<String> ids);

    Collection<Title> FindAllWithEqualDirectorAndWriterAndAlive(PageRequest<String> pageRequest);
}
