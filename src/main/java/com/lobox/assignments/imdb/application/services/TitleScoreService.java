package com.lobox.assignments.imdb.application.services;

import com.lobox.assignments.imdb.application.domain.models.TitleRating;
import com.lobox.assignments.imdb.application.domain.models.TitleScore;
import com.lobox.assignments.imdb.application.domain.repositories.TitleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@Service
public class TitleScoreService {
    private final Logger logger = LoggerFactory.getLogger(TitleScoreService.class);
    private static final int MINIMUM_VOTES = 15000;
    private final TitleRepository titleRepository;

    public TitleScoreService(TitleRepository titleRepository) {
        this.titleRepository = titleRepository;
    }

    public Map<String, Float> calculateTitlesScore(Collection<TitleRating> titleRatings) {
        logger.info("Calculating titles score");
        Map<String, Float> scores = new TreeMap<>();
        double averageRating = titleRatings.stream().mapToDouble(TitleRating::getAverageRating).average().getAsDouble();
        titleRatings.stream().map(r -> {
            double score = (r.getNumVotes() * r.getAverageRating() + averageRating * MINIMUM_VOTES) / (MINIMUM_VOTES + r.getNumVotes());
            return new TitleScore(r.getTitleId(), (float) score);
        }).forEach(titleScore -> scores.put(titleScore.titleId(), titleScore.score()));
        return scores;
    }

}
