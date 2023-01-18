package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Principal;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.application.domain.models.TitleRating;
import com.lobox.assignments.imdb.infrastructure.serialization.KryoObjectSerialization;
import com.lobox.assignments.imdb.infrastructure.serialization.ObjectSerialization;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RocksDbSerializations {
    private final ObjectSerialization objectSerialization;

    public RocksDbSerializations() {
        objectSerialization = new KryoObjectSerialization(List.of(Title.class, Person.class, Principal.class));
    }

    public byte[] serializeTitle(Title title) {
        return objectSerialization.serialize(title);
    }

    public Title deserializeTitle(byte[] value) {
        return objectSerialization.deserialize(value, Title.class);
    }

    public byte[] serializePerson(Person person) {
        return objectSerialization.serialize(person);
    }

    public Person deserializePerson(byte[] value) {
        return objectSerialization.deserialize(value, Person.class);
    }

    public byte[] serializePrincipal(Principal principal) {
        return objectSerialization.serialize(principal);
    }

    public Principal deserializePrincipal(byte[] value) {
        return objectSerialization.deserialize(value, Principal.class);
    }

    public byte[] serializeTitleRating(TitleRating rating) {
        return objectSerialization.serialize(rating);
    }

    public TitleRating deserializeTitleRating(byte[] value) {
        return objectSerialization.deserialize(value, TitleRating.class);
    }
}
