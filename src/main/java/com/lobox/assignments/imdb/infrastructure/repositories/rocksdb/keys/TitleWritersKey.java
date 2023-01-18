package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class TitleWritersKey implements RocksDbKey {
    private final String titleId;
    private final Collection<String> writers;
    private final String value;

    public static TitleWritersKey fromBytes(byte[] key) {
        String str = new String(key, StandardCharsets.UTF_8);
        String[] keyParts = str.split("\\|");
        String[] directors = keyParts[1].split(",");
        return new TitleWritersKey(keyParts[0], Arrays.stream(directors).toList());
    }

    public TitleWritersKey(String titleId, Collection<String> writers) {
        this.titleId = titleId;
        this.writers = writers;
        this.value = String.format("%s|%s", titleId, String.join(",", writers));
    }

    public static Collection<String> titleWriters(RocksIterator itr, String titleId) {
        return RocksDbKey.keysStartWith(itr, String.format("%s|", titleId))
                         .stream().findFirst().map(s -> TitleWritersKey.fromBytes(s.getBytes(StandardCharsets.UTF_8)))
                         .map(TitleWritersKey::getWriters)
                         .orElse(Collections.emptyList());
    }

    public String getTitleId() {
        return titleId;
    }

    public Collection<String> getWriters() {
        return writers;
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
