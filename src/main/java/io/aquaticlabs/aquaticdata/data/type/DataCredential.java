package io.aquaticlabs.aquaticdata.data.type;

import io.aquaticlabs.aquaticdata.data.ADatabase;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.type.mysql.MySQLDB;
import io.aquaticlabs.aquaticdata.data.type.sqlite.SQLiteDB;
import lombok.Getter;
import lombok.Setter;

import java.io.File;

/**
 * @Author: extremesnow
 * On: 8/21/2022
 * At: 23:34
 */
@Getter @Setter
public class DataCredential {

    private String databaseName;
    private String hostname;
    private int port = 3306;
    private String username;
    private String password;
    private boolean useSSL = false;
    private boolean allowPublicKeyRetrieval = true;
    private String tableName;
    private File folder;


    public DataCredential() {
    }

    public DataCredential MySQLCredential(String databaseName, String hostname, int port, String username, String password, String tableName) {
        this.databaseName = databaseName;
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        return this;
    }

    public DataCredential SQLiteCredential(File folder, String databaseName, String tableName) {
        this.folder = folder;
        this.databaseName = databaseName;
        this.tableName = tableName;
        return this;
    }


    public ADatabase build(DataObject object) {

        if (password != null) {
            return new MySQLDB(this, object);
        }
        return new SQLiteDB(this, object);

    }

    public DataCredential databaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public DataCredential hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public DataCredential port(int port) {
        this.port = port;
        return this;
    }

    public DataCredential username(String username) {
        this.username = username;
        return this;
    }

    public DataCredential password(String password) {
        this.password = password;
        return this;
    }

    public DataCredential tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
    public DataCredential folder(File folder) {
        this.folder = folder;
        if (!folder.exists()) folder.mkdirs();
        return this;
    }

}
