package com.instaclustr.model;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Notice that this is Java class, not Scala class
 *
 * If you work with Spark and you are using this class,
 * it does not matter how it is annotated (e.g. by Cassandra driver of version 4),
 *
 * what is important is that it has getters and setters and you have to use JavaBeanColumnMapper
 * if you want to save it via Spark
 *
 * The implemenation of Serializable interface is important!
 */
@Table(keyspace = "tests", name = "test")
public class TestModel implements Serializable {

    public static final String ID_COLUMN = "id";

    public static final String VALUE_COLUMN = "value";

    @PartitionKey
    @Column(name = ID_COLUMN)
    public UUID id;

    @Column(name = VALUE_COLUMN)
    public int value;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
