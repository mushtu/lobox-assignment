package com.lobox.assignments.imdb.application.dto;

import com.lobox.assignments.imdb.application.domain.models.Title;


public class ObjectMapper {
    public static TitleDto toTitleDto(Title title) {
        TitleDto titleDto = new TitleDto();
        titleDto.setId(title.getId());
        titleDto.setCast(title.getCast());
        titleDto.setDirectors(title.getDirectors());
        titleDto.setWriters(title.getWriters());
        titleDto.setAdult(title.getAdult());
        titleDto.setGenres(title.getGenres());
        titleDto.setOrdering(title.getOrdering());
        titleDto.setEndYear(title.getEndYear());
        titleDto.setPrimaryTitle(title.getPrimaryTitle());
        titleDto.setOriginalTitle(title.getOriginalTitle());
        titleDto.setStartYear(title.getStartYear());
        titleDto.setScore(title.getScore());
        titleDto.setTitleType(title.getTitleType());
        return titleDto;
    }
}
