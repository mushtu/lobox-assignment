package com.lobox.assignments.imdb.application.usecases;

import an.awesome.pipelinr.Command;
import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.application.domain.repositories.PersonRepository;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import com.lobox.assignments.imdb.application.dto.ObjectMapper;
import com.lobox.assignments.imdb.application.dto.TitleDto;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetTitlesWrittenDirectedByTheSameAlivePerson implements Command<Iterable<TitleDto>> {

    @Component
    public static class Handler implements Command.Handler<GetTitlesWrittenDirectedByTheSameAlivePerson, Iterable<TitleDto>> {
        private final TitleRepository titleRepository;
        private final PersonRepository personRepository;

        public Handler(TitleRepository titleRepository, PersonRepository personRepository) {
            this.titleRepository = titleRepository;
            this.personRepository = personRepository;
        }

        @Override
        public Iterable<TitleDto> handle(GetTitlesWrittenDirectedByTheSameAlivePerson command) {
            Collection<Title> titles = titleRepository.FindAllWithEqualDirectorAndWriter();
            List<String> personIds = titles.stream()
                    .map(t -> t.getDirectors().stream().findFirst().orElseThrow()).distinct().toList();
            Map<String, Person> alivePersons = personRepository.FindAllWithIdsAndAlive(personIds).stream()
                    .collect(Collectors.toMap(Person::getId, person -> person));
            return titles.stream().filter(t -> alivePersons.containsKey(t.getDirectors().stream().findFirst().orElseThrow()))
                    .map(ObjectMapper::toTitleDto).collect(Collectors.toList());
        }
    }
}
