package com.lobox.assignments.imdb.application.domain.repositories;

import com.lobox.assignments.imdb.application.domain.models.PageRequest;
import com.lobox.assignments.imdb.application.domain.models.Title;

import java.util.Collection;
import java.util.Optional;

public interface TitleRepository {
    Optional<Title> findById(String id);

    Collection<Title> findAllByIds(Iterable<String> ids);

    Collection<Title> findAllWithEqualDirectorAndWriterAndAlive(PageRequest<String> pageRequest);

    Iterable<Title> findActorsCommonTitles(String firstActorId, String secondActorId);
}
