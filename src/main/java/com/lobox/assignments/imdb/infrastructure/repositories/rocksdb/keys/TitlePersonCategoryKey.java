package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import com.lobox.assignments.imdb.application.domain.models.Principal;

import java.nio.charset.StandardCharsets;

/**
 * Composite key with the format of {titleId}|{personId}|{category}
 */
public class TitlePersonCategoryKey implements RocksDbKey {
    private final String personId;
    private final String titleId;
    private final String category;
    private final String value;

    public static TitlePersonCategoryKey fromPrincipal(Principal principal) {
        return new TitlePersonCategoryKey(principal.getPersonId(), principal.getTitleId(), principal.getCategory());
    }

    public TitlePersonCategoryKey(String personId, String titleId, String category) {
        this.personId = personId;
        this.titleId = titleId;
        this.category = category;
        this.value = String.format("%s|%s|%s", titleId, personId, category);
    }

    public String getPersonId() {
        return personId;
    }

    public String getTitleId() {
        return titleId;
    }

    public String getCategory() {
        return category;
    }

    @Override
    public byte[] toBytes() {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return value;
    }
}
