package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TitleDirectorsKey implements RocksDbKey {
    private final String titleId;
    private final Collection<String> directors;
    private final String value;

    public static TitleDirectorsKey fromBytes(byte[] key) {
        String str = new String(key, StandardCharsets.UTF_8);
        String[] keyParts = str.split("\\|");
        String[] directors = keyParts[1].split(",");
        return new TitleDirectorsKey(keyParts[0], Arrays.stream(directors).toList());
    }

    public static Collection<String> titleDirectors(RocksIterator itr, String titleId) {
        return RocksDbKey.keysStartWith(itr, String.format("%s|", titleId))
                         .stream().findFirst().map(s -> TitleDirectorsKey.fromBytes(s.getBytes(StandardCharsets.UTF_8)))
                         .map(TitleDirectorsKey::getDirectors)
                         .orElse(Collections.emptyList());
    }

    public TitleDirectorsKey(String titleId, Collection<String> directors) {
        this.titleId = titleId;
        this.directors = directors;
        this.value = String.format("%s|%s", titleId, String.join(",", directors));
    }

    public String getTitleId() {
        return titleId;
    }

    public Collection<String> getDirectors() {
        return directors;
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
