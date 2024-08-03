package org.megamaind.scyllacache;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableWithOptionsEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateTable;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.lang.NonNull;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterTable;

@Slf4j
public class ScyllaCacheManager implements CacheManager, BeanClassLoaderAware {

    private final CqlSession cqlSession;
    private final String cacheTable;
    private final Hashtable<String, Cache> cacheMap = new Hashtable<>(32);

    public ScyllaCacheManager(CqlSession cqlSession, String table) {
        this.cqlSession = cqlSession;
        this.cacheTable = table;
    }

    public ScyllaCacheManager(String host, String dataCenter, int port, String keyspace, String table){
        this.cqlSession = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql(keyspace))
                .withLocalDatacenter(dataCenter)
                .addContactPoints(getContactPoints(host, port))
                .build();
        this.cacheTable = table;
        createTable(cqlSession, keyspace, table);
    }
    public ScyllaCacheManager withTtl(int ttl){
        AlterTableWithOptionsEnd alterTableWithOptionsEnd = alterTable(CqlIdentifier.fromCql(cacheTable)).withDefaultTimeToLiveSeconds(ttl);
        cqlSession.execute(alterTableWithOptionsEnd.build());
        log.info("TTL set to: {}", ttl);
        return this;
    }

    private void createTable(CqlSession session, String keyspace, String cacheTable){
        CreateTable createTable =
                new DefaultCreateTable(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(cacheTable),
                        true, false,
                        columnsInOrder(), partitionKeyColumns(),
                        clusteringKeyColumns(), ImmutableSet.of(),ImmutableSet.of(),
                        ImmutableMap.of(), ImmutableMap.of());

        session.execute(createTable.build());
    }
    private Collection<InetSocketAddress> getContactPoints(String host, int port){
        return List.of(new InetSocketAddress[]{new InetSocketAddress(host, port)});
    }
    public ScyllaCacheManager withCaches(String... caches){
        for (String cache : caches) {
            cacheMap.put(cache, new ScyllaCache(cache, true, cqlSession, cacheTable));
        }
        return this;
    }

    @Override
    public Cache getCache(@NonNull String name) {
        return cacheMap.computeIfAbsent(name, key -> new ScyllaCache(name, true, cqlSession, cacheTable));
    }

    @Override
    @NonNull
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(cacheMap.keySet());
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {

    }

    private ImmutableMap<CqlIdentifier, DataType> columnsInOrder(){
        return ImmutableMap.<CqlIdentifier, DataType>builder()
                .put(CqlIdentifier.fromCql("cache"), DataTypes.TEXT)
                .put(CqlIdentifier.fromCql("key"), DataTypes.TEXT)
                .put(CqlIdentifier.fromCql("value"), DataTypes.TEXT)
                .build();

    }
    private ImmutableSet<CqlIdentifier> partitionKeyColumns(){
        return ImmutableSet.<CqlIdentifier>builder()
                .add(CqlIdentifier.fromCql("cache"))
                .build();
    }
    ImmutableSet<CqlIdentifier> clusteringKeyColumns(){
        return ImmutableSet.<CqlIdentifier>builder()
                .add(CqlIdentifier.fromCql("key"))
                .build();
    }

}
