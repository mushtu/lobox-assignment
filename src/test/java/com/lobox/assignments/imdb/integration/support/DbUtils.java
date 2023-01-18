package com.lobox.assignments.imdb.integration.support;

import com.lobox.assignments.imdb.application.domain.models.Person;
import com.lobox.assignments.imdb.application.domain.models.Title;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDatabase;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.RocksDbSerializations;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys.PrimaryKey;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys.TitleDirectorsKey;
import com.lobox.assignments.imdb.infrastructure.repositories.rocksdb.keys.TitleWritersKey;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

public class DbUtils {
    static RocksDbSerializations serializations = new RocksDbSerializations();

    public static void indexTitles(RocksDatabase rocks, Collection<Title> titles) throws RocksDBException {

        for (Title title : titles) {
            rocks.db().put(rocks.titlesPrimaryIndex(), PrimaryKey.fromString(title.getId()).toBytes(), serializations.serializeTitle(title));
            rocks.db().put(rocks.titlesSecondaryIndexWriters(), new TitleWritersKey(title.getId(), title.getWriters()).toBytes(), new byte[0]);
            rocks.db().put(rocks.titlesSecondaryIndexDirectors(), new TitleDirectorsKey(title.getId(), title.getDirectors()).toBytes(), new byte[0]);
        }

    }

    public static void indexPersons(RocksDatabase rocks, Set<Person> persons) throws RocksDBException {
        for (Person p : persons) {
            rocks.db().put(rocks.personsPrimaryIndex(), PrimaryKey.fromString(p.getId()).toBytes(), serializations.serializePerson(p));
            if (p.getDeathYear() != null) {
                rocks.db().put(rocks.personsSecondaryIndexDeathYear(), PrimaryKey.fromString(p.getId()).toBytes(), ByteBuffer.allocate(4).putInt(p.getDeathYear()).array());
            }
        }
    }
}
