package testing;

import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.SerializedData;
import lombok.Getter;

import java.util.UUID;

/**
 * @Author: extremesnow
 * On: 8/24/2022
 * At: 21:16
 */
@Getter
public class TestData implements DataObject {

    public UUID uuid;
    public String name = "Test Object";
    //private Map<StatType, Stat> statMap;
    private int level = 0;
    private int stat1 = 0;
    private int stat2 = 0;
    private int stat3 = 0;


    public TestData() {
        //populateStatMap();
    }


    public TestData(UUID uuid, String name, int level) {
        this.uuid = uuid;
        this.name = name;
        this.level = level;
        //populateStatMap();
    }

    @Override
    public Object getKey() {
        return uuid;
    }

    @Override
    public void serialize(SerializedData data) {
        data.write("uuid", uuid);
        data.write("name", name);
        data.write("level", level);
        data.write("levelRank", 0);
        data.write("stat1", stat1);
        data.write("stat2", stat2);
        data.write("stat3", stat3);
        System.out.println("Serialize: " + data.getValueTypes());

    }

    @Override
    public void deserialize(SerializedData data) {
        uuid = data.applyAs("uuid", UUID.class);
        name = data.applyAs("name", String.class);
        level = data.applyAs("level", Integer.class);
        stat1 = data.applyAs("stat1", Integer.class);
        stat2 = data.applyAs("stat2", Integer.class);
        stat3 = data.applyAs("stat3", Integer.class);

        System.out.println("Deserialize: " + data.getValueTypes());
    }

    @Override
    public Object getDefaultDataValue(String columnName) {
        switch (columnName) {
            case "uuid":
            case "name":
                return "";
            default:
                return 0;
        }
    }
}
