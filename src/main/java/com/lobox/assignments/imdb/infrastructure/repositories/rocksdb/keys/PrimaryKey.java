package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import java.nio.charset.StandardCharsets;

public class PrimaryKey implements RocksDbKey {
    private final String value;

    public static PrimaryKey fromBytes(byte[] key) {
        return new PrimaryKey(new String(key, StandardCharsets.UTF_8));
    }

    public static PrimaryKey fromString(String value) {
        return new PrimaryKey(value);
    }

    public PrimaryKey(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
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
