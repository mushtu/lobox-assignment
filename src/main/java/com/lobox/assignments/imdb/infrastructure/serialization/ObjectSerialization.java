package com.lobox.assignments.imdb.infrastructure.serialization;

public interface ObjectSerialization {
    <T> byte[] serialize(T object);

    <T> T deserialize(byte[] bytes, Class<T> tClass);

}
