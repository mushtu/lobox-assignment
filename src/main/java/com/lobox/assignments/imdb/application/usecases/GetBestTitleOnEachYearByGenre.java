package com.lobox.assignments.imdb.application.usecases;

import an.awesome.pipelinr.Command;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import com.lobox.assignments.imdb.application.dto.ObjectMapper;
import com.lobox.assignments.imdb.application.dto.TitleDto;
import com.lobox.assignments.imdb.application.services.TitleScoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

public class GetBestTitleOnEachYearByGenre implements Command<Map<Integer, TitleDto>> {
    private final String genre;

    public GetBestTitleOnEachYearByGenre(String genre) {
        this.genre = genre;
    }

    @Component
    public static class Handler implements Command.Handler<GetBestTitleOnEachYearByGenre, Map<Integer, TitleDto>> {

        private final Logger logger = LoggerFactory.getLogger(Handler.class);
        private final TitleRepository titleRepository;
        private final TitleScoreService titleScoreService;

        public Handler(TitleRepository titleRepository, TitleScoreService titleScoreService) {
            this.titleRepository = titleRepository;
            this.titleScoreService = titleScoreService;
        }

        @Override
        public Map<Integer, TitleDto> handle(GetBestTitleOnEachYearByGenre command) {
            return titleRepository.findBestTitleOnEachYearByGenre(command.genre).entrySet().stream()
                                  .collect(Collectors.toMap(Map.Entry::getKey, entry -> ObjectMapper.toTitleDto(entry.getValue())));
        }

    }


}
