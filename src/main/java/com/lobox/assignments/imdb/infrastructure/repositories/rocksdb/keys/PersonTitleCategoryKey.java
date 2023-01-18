package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import com.lobox.assignments.imdb.application.domain.models.Principal;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Composite key with the format of {personId}|{titleId}|{category}
 */
public class PersonTitleCategoryKey implements RocksDbKey {
    private final String personId;
    private final String titleId;
    private final String category;
    private final String value;

    public static Collection<PersonTitleCategoryKey> keysStartWithPersonEndWithCategory(RocksIterator itr, String personId, String category) {
        return RocksDbKey.keysStartWith(itr, String.format("%s|", personId)).stream()
                         .filter(s -> s.endsWith(String.format("|%s", category)))
                         .map(key -> fromBytes(key.getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());

    }

    public static PersonTitleCategoryKey fromBytes(byte[] bytes) {
        String key = new String(bytes, StandardCharsets.UTF_8);
        String[] keyParts = key.split("\\|");
        return new PersonTitleCategoryKey(keyParts[0], keyParts[1], keyParts[2]);
    }

    public static PersonTitleCategoryKey fromPrincipal(Principal principal) {
        return new PersonTitleCategoryKey(principal.getPersonId(), principal.getTitleId(), principal.getCategory());
    }

    public PersonTitleCategoryKey(String personId, String titleId, String category) {
        this.personId = personId;
        this.titleId = titleId;
        this.category = category;
        this.value = String.format("%s|%s|%s", personId, titleId, category);
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
