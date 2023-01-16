package com.lobox.assignments.imdb.application.domain.models;

import java.io.Serializable;
import java.util.Collection;

public class Person implements Serializable {
    private String id;
    private String primaryName;
    private Integer birthYear;
    private Integer deathYear;
    private Collection<String> primaryProfession;
    private Collection<String> knownForTitles;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPrimaryName() {
        return primaryName;
    }

    public void setPrimaryName(String primaryName) {
        this.primaryName = primaryName;
    }

    public Integer getBirthYear() {
        return birthYear;
    }

    public void setBirthYear(Integer birthYear) {
        this.birthYear = birthYear;
    }

    public Integer getDeathYear() {
        return deathYear;
    }

    public void setDeathYear(Integer deathYear) {
        this.deathYear = deathYear;
    }

    public Collection<String> getPrimaryProfession() {
        return primaryProfession;
    }

    public void setPrimaryProfession(Collection<String> primaryProfession) {
        this.primaryProfession = primaryProfession;
    }

    public Collection<String> getKnownForTitles() {
        return knownForTitles;
    }

    public void setKnownForTitles(Collection<String> knownForTitles) {
        this.knownForTitles = knownForTitles;
    }
}
