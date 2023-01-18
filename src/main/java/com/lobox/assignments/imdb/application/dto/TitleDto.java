package com.lobox.assignments.imdb.application.dto;

import java.util.Collection;

public class TitleDto {
    private String id;
    private Integer ordering;
    private String primaryTitle;
    private String originalTitle;
    private Boolean isAdult;
    private Integer startYear;
    private Integer endYear;
    private Integer runtimeMinutes;
    private Collection<String> genres;
    private Collection<String> directors;
    private Collection<String> writers;
    private Collection<String> cast;
    private Float score;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getOrdering() {
        return ordering;
    }

    public void setOrdering(Integer ordering) {
        this.ordering = ordering;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public void setPrimaryTitle(String primaryTitle) {
        this.primaryTitle = primaryTitle;
    }

    public String getOriginalTitle() {
        return originalTitle;
    }

    public void setOriginalTitle(String originalTitle) {
        this.originalTitle = originalTitle;
    }

    public Boolean getAdult() {
        return isAdult;
    }

    public void setAdult(Boolean adult) {
        isAdult = adult;
    }

    public Integer getStartYear() {
        return startYear;
    }

    public void setStartYear(Integer startYear) {
        this.startYear = startYear;
    }

    public Integer getEndYear() {
        return endYear;
    }

    public void setEndYear(Integer endYear) {
        this.endYear = endYear;
    }

    public Integer getRuntimeMinutes() {
        return runtimeMinutes;
    }

    public void setRuntimeMinutes(Integer runtimeMinutes) {
        this.runtimeMinutes = runtimeMinutes;
    }

    public Collection<String> getGenres() {
        return genres;
    }

    public void setGenres(Collection<String> genres) {
        this.genres = genres;
    }

    public Collection<String> getDirectors() {
        return directors;
    }

    public void setDirectors(Collection<String> directors) {
        this.directors = directors;
    }

    public Collection<String> getWriters() {
        return writers;
    }

    public void setWriters(Collection<String> writers) {
        this.writers = writers;
    }

    public Collection<String> getCast() {
        return cast;
    }

    public void setCast(Collection<String> cast) {
        this.cast = cast;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }
}
