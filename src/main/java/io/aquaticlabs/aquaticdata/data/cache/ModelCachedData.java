package io.aquaticlabs.aquaticdata.data.cache;

import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));

      // Take only the first 4 bytes (32 bits) of the hash
      int result = 0;
      for (int i = 0; i < Math.min(4, hash.length); i++) {
        result = (result << 8) | (hash[i] & 0xFF);
      }
      return result;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }
}