package com.lobox.assignments.imdb.infrastructure.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoObjectSerialization implements ObjectSerialization {
    private final Kryo kryo;

    public KryoObjectSerialization(Kryo kryo) {
        this.kryo = kryo;
    }

    public KryoObjectSerialization(Iterable<Class<?>> classes) {
        this.kryo = new Kryo();
        classes.forEach(kryo::register);
        this.kryo.setRegistrationRequired(false);
    }

    @Override
    public <T> byte[] serialize(T object) {
        Output output = new Output(1024, -1);
        kryo.writeObject(output, object);
        return output.getBuffer();
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> tClass) {
        Input input = new Input(bytes);
        return kryo.readObject(input, tClass);
    }
}
