package com.lobox.assignments.imdb.api;

import an.awesome.pipelinr.Pipeline;
import com.lobox.assignments.imdb.application.domain.models.PageRequest;
import com.lobox.assignments.imdb.application.dto.TitleDto;
import com.lobox.assignments.imdb.application.usecases.GetActorsCommonTitles;
import com.lobox.assignments.imdb.application.usecases.GetBestTitleOnEachYearByGenre;
import com.lobox.assignments.imdb.application.usecases.GetTitlesWrittenDirectedByTheSameAlivePerson;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/titles")
public class TitlesController {
    private final Pipeline pipeline;

    public TitlesController(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @GetMapping("written-directed-same-alive-person")
    public Iterable<TitleDto> GetTitlesWrittenDirectedByTheSameAlivePerson(PageRequest<String> pageRequest) {
        return new GetTitlesWrittenDirectedByTheSameAlivePerson(pageRequest).execute(pipeline);
    }

    @GetMapping("actors-common-titles")
    public Iterable<TitleDto> GetActorsCommonTitles(@RequestParam String firstActorId, @RequestParam String secondActorId) {
        return new GetActorsCommonTitles(firstActorId, secondActorId).execute(pipeline);
    }

    @GetMapping("genre-best-titles")
    public Map<Integer, TitleDto> GetGenreBestTitles(@RequestParam String genre) {
        return new GetBestTitleOnEachYearByGenre(genre).execute(pipeline);
    }
}
