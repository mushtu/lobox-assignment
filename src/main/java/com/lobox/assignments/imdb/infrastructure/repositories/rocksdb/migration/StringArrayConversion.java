package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.migration;

import com.univocity.parsers.conversions.ObjectConversion;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class StringArrayConversion extends ObjectConversion<Collection<String>> {
    @Override
    protected Collection<String> fromString(String input) {
        return Arrays.stream(input.split(",")).map(String::trim).distinct().collect(Collectors.toList());
    }
}
