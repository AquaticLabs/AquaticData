package io.aquaticlabs.aquaticdata.type;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.NonNull;

import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 8/21/2022
 * At: 23:34
 */

public interface DataCredential {

    String getDatabaseName();

    String getTableName();

    <T extends StorageModel> Database<T> build(DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor);

}
