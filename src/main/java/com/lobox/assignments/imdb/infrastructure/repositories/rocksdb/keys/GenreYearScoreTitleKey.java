package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import com.lobox.assignments.imdb.application.domain.models.Title;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Composite key with the format of {genre}|{year}|{score}|{titleId}
 */
public class GenreYearScoreTitleKey implements RocksDbKey {

    private final String genre;
    private final Integer year;
    private final Float score;
    private final String titleId;
    private final String value;

    public static byte[] withPrefix(String genre, Integer year) {
        return String.format("%s|%s|", genre, year).getBytes(StandardCharsets.UTF_8);
    }

    public static GenreYearScoreTitleKey fromBytes(byte[] bytes) {
        String key = new String(bytes, StandardCharsets.UTF_8);
        String[] keyParts = key.split("\\|");
        String genre = keyParts[0];
        Integer year = Integer.parseInt(keyParts[1]);
        String titleId = keyParts[3];
        Float score = Float.parseFloat(keyParts[2]);
        return new GenreYearScoreTitleKey(titleId, genre, year, score);
    }

    public static Collection<GenreYearScoreTitleKey> fromTitle(Title title) {
        if (title.getGenres() != null && title.getStartYear() != null && title.getScore() != null) {
            return title.getGenres().stream()
                        .map(genre ->
                                new GenreYearScoreTitleKey(
                                        title.getId(),
                                        genre,
                                        title.getStartYear(),
                                        title.getScore()))
                        .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public GenreYearScoreTitleKey(String titleId, String genre, Integer year, Float score) {
        this.titleId = titleId;
        this.genre = genre;
        this.year = year;
        this.score = score;
        this.value = String.format("%s|%s|%s|%s", genre, year, score, titleId);
    }

    public String getTitleId() {
        return titleId;
    }

    public String getGenre() {
        return genre;
    }

    public Integer getYear() {
        return year;
    }

    public Float getScore() {
        return score;
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
