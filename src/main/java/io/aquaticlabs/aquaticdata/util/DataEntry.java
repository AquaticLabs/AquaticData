package io.aquaticlabs.aquaticdata.util;

/**
 * @Author: extremesnow
 * On: 8/20/2022
 * At: 22:26
 */
public class DataEntry<K, V> {

    private K key;
    private V value;

    public DataEntry() {
    }

    public DataEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public DataEntry<K, V> key(K key) {
        this.key = key;
        return this;
    }

    public DataEntry<K, V> value(V value) {
        this.value = value;
        return this;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }
}
