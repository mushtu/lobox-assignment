package com.lobox.assignments.imdb.application.domain.models;

import java.io.Serializable;

public class Principal implements Serializable {
    private String titleId;
    private Integer titleOrdering;
    private String personId;
    private String category;
    private String job;
    private String characters;

    public String getTitleId() {
        return titleId;
    }

    public void setTitleId(String titleId) {
        this.titleId = titleId;
    }

    public Integer getTitleOrdering() {
        return titleOrdering;
    }

    public void setTitleOrdering(Integer titleOrdering) {
        this.titleOrdering = titleOrdering;
    }

    public String getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = personId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getCharacters() {
        return characters;
    }

    public void setCharacters(String characters) {
        this.characters = characters;
    }
}
