package io.aquaticlabs.aquaticdata.type.json;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.type.DataCredential;

import java.io.File;
import java.util.concurrent.Executor;

public class JsonCredential  implements DataCredential {

    private final String databaseName;
    private final String tableName;
    private final File file;

    public JsonCredential(String databaseName, String tableName, File file) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public <T extends StorageModel> Database<T> build(DatabaseStructure tableStructure, Serializer<T> serializer, Executor asyncExecutor, Executor syncExecutor) {
        return new JsonDatabase<>(this, tableStructure, serializer, asyncExecutor, syncExecutor);
    }

}
