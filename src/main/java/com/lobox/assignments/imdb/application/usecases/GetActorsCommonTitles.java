package com.lobox.assignments.imdb.application.usecases;

import an.awesome.pipelinr.Command;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import com.lobox.assignments.imdb.application.dto.ObjectMapper;
import com.lobox.assignments.imdb.application.dto.TitleDto;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GetActorsCommonTitles implements Command<Iterable<TitleDto>> {
    private final String firstActorId;
    private final String secondActorId;

    public GetActorsCommonTitles(String firstActorId, String secondActorId) {
        this.firstActorId = firstActorId;
        this.secondActorId = secondActorId;
    }

    @Component
    public static class Handler implements Command.Handler<GetActorsCommonTitles, Iterable<TitleDto>> {

        private final TitleRepository titleRepository;

        public Handler(TitleRepository titleRepository) {
            this.titleRepository = titleRepository;
        }

        @Override
        public Iterable<TitleDto> handle(GetActorsCommonTitles command) {
            return StreamSupport.stream(titleRepository.findActorsCommonTitles(command.firstActorId, command.secondActorId).spliterator(), false)
                    .map(ObjectMapper::toTitleDto).collect(Collectors.toList());
        }
    }
}
