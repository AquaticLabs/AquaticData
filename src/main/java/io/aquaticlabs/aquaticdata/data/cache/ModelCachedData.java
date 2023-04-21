package io.aquaticlabs.aquaticdata.data.cache;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class ModelCachedData {
  @Getter
  private final Map<String, Integer> cache = new HashMap<>();

  public void clear() {
    cache.clear();
  }

  public void add(String field, String data) {
    cache.put(field, hashString(data));
  }

  public boolean isEmpty() {
    return cache.isEmpty();
  }

  public boolean isOutdated(String field, String newData) {
    int newDataHash = hashString(newData);

    Integer hash = cache.get(field);
    if (hash == null) {
      cache.put(field, newDataHash);
      return true;
    }
    if (hash == newDataHash) return false;

    cache.remove(field);
    cache.put(field, newDataHash);
    return true;
  }

  public int hashString(String data) {
    int hash = 0;
    for (char c : data.toCharArray()) hash += Character.hashCode(c);

    return hash;
  }
}