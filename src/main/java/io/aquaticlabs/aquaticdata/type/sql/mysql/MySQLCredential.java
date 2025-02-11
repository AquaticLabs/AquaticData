package io.aquaticlabs.aquaticdata.type.sql.mysql;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.type.sql.SQLCredential;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:39
 */


@RequiredArgsConstructor
@Getter
public class MySQLCredential implements SQLCredential {

    private final String databaseName;
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final String tableName;

    @Setter
    private boolean useSSL = false;
    @Setter
    private boolean allowPublicKeyRetrieval = true;

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public <T extends StorageModel> Database<T> build(DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        return new MySQLDatabase<>(this, tableStructure, serializer, asyncExecutor, syncExecutor);
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}
