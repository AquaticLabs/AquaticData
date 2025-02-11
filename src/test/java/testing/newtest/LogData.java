package testing.newtest;

import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 14:45
 */
@Getter @Setter
public class LogData implements StorageModel, Comparable<LogData> {

    private int alertID;
    private long timestamp;
    private UUID playerUUID;
    private String playerName;
    private CheckType checkType;
    private String moduleName;
    private CheckData checkData;

    public LogData() {}

    public LogData(int alertID, UUID playerUUID, String playerName, CheckType checkType, String moduleName, CheckData checkData) {
        this.alertID = alertID;
        this.timestamp = checkData.getTimestamp();
        this.playerUUID = playerUUID;
        this.playerName = playerName;
        this.checkType = checkType;
        this.moduleName = moduleName;
        this.checkData = checkData;
    }

    @Override
    public Object getKey() {
        return alertID;
    }



    public long getTimestamp() {
        return timestamp;
    }

    public UUID getPlayerUUID() {
        return playerUUID;
    }

    public boolean isEqual(LogData comparing) {
        return alertID == comparing.alertID && playerUUID == comparing.playerUUID;
    }

    public int getAlertID() {
        return alertID;
    }

    public String getPlayerName() {
        return playerName;
    }

    public CheckType getCheckType() {
        return checkType;
    }

    public CheckData getCheckData() {
        return checkData;
    }

    public String getModuleName() {
        return moduleName;
    }

    @Override
    public int compareTo(LogData otherData) {
        return Long.compare(this.timestamp, otherData.timestamp);
    }



}
