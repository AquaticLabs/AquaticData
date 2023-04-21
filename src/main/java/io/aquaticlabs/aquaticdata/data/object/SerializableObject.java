package io.aquaticlabs.aquaticdata.data.object;

import io.aquaticlabs.aquaticdata.data.storage.SerializedData;

public interface SerializableObject {
  void serialize(SerializedData data);

  void deserialize(SerializedData data);

}