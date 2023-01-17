package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Principal;
import com.lobox.assignments.imdb.application.domain.models.Title;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

@Component
public class RocksDbSerializations {
    public byte[] serializeTitle(Title title) {
        return SerializationUtils.serialize(title);
    }

    public Title deserializeTitle(byte[] value) {
        return (Title) SerializationUtils.deserialize(value);
    }

    public byte[] serializePerson(Person person) {
        return SerializationUtils.serialize(person);
    }

    public Person deserializePerson(byte[] value) {
        return (Person) SerializationUtils.deserialize(value);
    }

    public byte[] serializePrincipal(Principal principal) {
        return SerializationUtils.serialize(principal);
    }

    public Principal deserializePrincipal(byte[] value) {
        return (Principal) SerializationUtils.deserialize(value);
    }
}
