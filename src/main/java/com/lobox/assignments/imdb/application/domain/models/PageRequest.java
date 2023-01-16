package com.lobox.assignments.imdb.application.domain.models;

public class PageRequest<TKey> {
    private int pageSize = 20;
    private String lastKey;

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getLastKey() {
        return lastKey;
    }

    public void setLastKey(String lastKey) {
        this.lastKey = lastKey;
    }
}
