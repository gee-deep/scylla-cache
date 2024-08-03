package org.megamaind.scyllacache;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.io.*;
import java.util.Base64;
import java.util.concurrent.Callable;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class ScyllaCache extends AbstractValueAdaptingCache {

    private final String cacheName;
    private final CqlSession cqlSession;
    private final String table;

    public ScyllaCache(String name, boolean allowNullValues, CqlSession cqlSession, String table) {
        super(allowNullValues);
        this.cqlSession = cqlSession;
        this.table = table;
        this.cacheName = name;
    }

    @Override
    @Nullable
    protected Object lookup(Object key) {
        return fromStoreValue(getFromScylla(key.toString()));
    }

    @Override
    @NonNull
    public String getName() {
        return this.cacheName;
    }

    @Override
    public Object getNativeCache() {
        return null;
    }

    @Override
    @Nullable
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = lookup(key);
        if (value != null) {
            return (T) fromStoreValue(value);
        }

        try {
            T newValue = valueLoader.call();
            put(key, newValue);
            return newValue;
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    @Override
    public void put(Object key, @Nullable Object value) {
        Object storeValue = toStoreValue(value);
        String serializedValue = serialize(storeValue);

        SimpleStatement statement = insertInto(table)
                .value("cache", literal(cacheName))
                .value("key", literal(key.toString()))
                .value("value", literal(serializedValue))
                .build();
        cqlSession.execute(statement);
    }

    @Override
    @Nullable
    public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {
        Object existingValue = lookup(key);
        if (existingValue == null) {
            put(key, value);
            return null;
        }
        return toValueWrapper(existingValue);
    }

    @Override
    public void evict(Object key) {
        SimpleStatement statement = deleteFrom(table)
                .whereColumn("cache").isEqualTo(literal(cacheName))
                .whereColumn("key").isEqualTo(literal(key.toString()))
                .build();
        cqlSession.execute(statement);
    }

    @Override
    public void clear() {
        SimpleStatement statement = deleteFrom(table)
                .whereColumn("cache").isEqualTo(literal(cacheName))
                .build();
        cqlSession.execute(statement);
    }

    @Nullable
    private String getFromScylla(String cacheKey) {
        SimpleStatement statement = selectFrom(table)
                .column("value")
                .whereColumn("cache").isEqualTo(literal(cacheName))
                .whereColumn("key").isEqualTo(literal(cacheKey))
                .build();
        Row row = cqlSession.execute(statement).one();
        return row != null ? row.getString("value") : null;
    }

    private String serialize(Object obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    private Object deserialize(String str) {
        byte[] bytes = Base64.getDecoder().decode(str);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize object", e);
        }
    }


    @Override
    protected Object fromStoreValue(Object storeValue) {
        if (storeValue == null) {
            return null;
        }
        if (storeValue instanceof String) {
            return deserialize((String) storeValue);
        }
        return storeValue;
    }

    @Override
    protected Object toStoreValue(@Nullable Object userValue) {
        Object storeValue = super.toStoreValue(userValue);
        if (storeValue == null) {
            return null;
        }
        return serialize(storeValue);
    }
}
