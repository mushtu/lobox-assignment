package com.lobox.assignments.imdb.application.usecases;

import an.awesome.pipelinr.Command;
import com.lobox.assignments.imdb.application.domain.models.PageRequest;
import com.lobox.assignments.imdb.application.domain.repositories.PersonRepository;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import com.lobox.assignments.imdb.application.dto.ObjectMapper;
import com.lobox.assignments.imdb.application.dto.TitleDto;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

public class GetTitlesWrittenDirectedByTheSameAlivePerson implements Command<Iterable<TitleDto>> {

    private final PageRequest<String> pageRequest;

    public GetTitlesWrittenDirectedByTheSameAlivePerson(PageRequest<String> pageRequest) {
        this.pageRequest = pageRequest;
    }

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
            return titleRepository.findAllWithEqualDirectorAndWriterAndAlive(command.pageRequest)
                    .stream().map(ObjectMapper::toTitleDto).collect(Collectors.toList());
        }
    }
}
