package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.util.BytewiseComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public interface RocksDbKey extends Comparable<RocksDbKey> {
    byte[] toBytes();

    @Override
    default int compareTo(RocksDbKey o) {
        return toString().compareTo(o.toString());

    }

    default int bytewiseCompareTo(RocksDbKey o) {
        BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
        return comparator.compare(ByteBuffer.wrap(toBytes()), ByteBuffer.wrap(o.toBytes()));
    }

    static List<String> keysStartWith(RocksIterator itr, String prefix) {
        List<String> keys = new ArrayList<>();
        itr.seek(prefix.getBytes());
        while (itr.isValid()) {
            String key = new String(itr.key());
            if (!key.startsWith(prefix))
                break;
            keys.add(key);
            itr.next();
        }
        return keys;
    }
}
