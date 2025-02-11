package io.aquaticlabs.aquaticdata.type.sql.sqlite;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.type.sql.SQLCredential;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.io.File;
import java.util.concurrent.Executor;

@AllArgsConstructor
@Getter
public class SQLiteCredential implements SQLCredential {

    private String databaseName;
    private String tableName;
    private File folder;

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public <T extends StorageModel> Database<T> build(DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        return new SQLiteDatabase<>(this, tableStructure, serializer, asyncExecutor, syncExecutor);
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}