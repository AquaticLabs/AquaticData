package testing;

import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.data.storage.SerializedData;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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

/*    private void populateStatMap() {
        if (statMap == null) {
            statMap = new HashMap<>();
        }
        for (StatType type : StatType.values()) {
            if (!statMap.containsKey(type) || statMap.get(type) == null) {
                Stat stat = new Stat(type, type.getDefaultValue());
                statMap.put(type, stat);
            }
        }
    }*/

/*    @Override
    public String toString() {

        List<String> stringList = statMap.entrySet().stream()
                .map(entry -> entry.getKey().toString() + ", " + entry.getValue().getValue())
                .collect(Collectors.toList());

        return "TestData{" +
                "uuid=" + uuid +
                ", name='" + name + '\'' +
                ", statMap=" + stringList.toString() +
                '}';
    }*/

/*    public Stat getStat(StatType type) {
        return statMap.get(type);
    }*/

    @Override
    public Object getKey() {
        return uuid;
    }

    @Override
    public ArrayList<DataEntry<String, ColumnType>> getStructure() {
        ArrayList<DataEntry<String, ColumnType>> structure = new ArrayList<>();

        structure.add(new DataEntry<>("uuid", ColumnType.VARCHAR_UUID));
        structure.add(new DataEntry<>("name", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("level", ColumnType.INT));
        structure.add(new DataEntry<>("stat1", ColumnType.INT));
        structure.add(new DataEntry<>("stat2", ColumnType.INT));
        structure.add(new DataEntry<>("stat3", ColumnType.INT));

        return structure;
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


    }

    @Override
    public void deserialize(SerializedData data) {
        uuid = data.applyAs("uuid", UUID.class);
        name = data.applyAs("name", String.class);
        level = data.applyAs("level", Integer.class);
        stat1 = data.applyAs("stat1", Integer.class);
        stat2 = data.applyAs("stat2", Integer.class);
        stat3 = data.applyAs("stat3", Integer.class);
    }

    /*


    @Override
    public void serialize(SerializedData data) {
        data.write("uuid", uuid);
        data.write("name", name);
        data.write("level", getStat(StatType.LEVEL).getValue());
        data.write("level_rank", getStat(StatType.LEVEL).getRank());
        data.write("kills", getStat(StatType.KILL).getValue());
        data.write("kills_rank", getStat(StatType.KILL).getRank());
        data.write("deaths", getStat(StatType.DEATH).getValue());
        data.write("deaths_rank", getStat(StatType.DEATH).getRank());
        data.write("experience", getStat(StatType.EXPERIENCE).getValue());
        data.write("experience_rank", getStat(StatType.EXPERIENCE).getRank());
        data.write("kill_streak", getStat(StatType.KILL_STREAK).getValue());
        data.write("kill_streak_rank", getStat(StatType.KILL_STREAK).getRank());
        data.write("best_kill_streak", getStat(StatType.BEST_KILL_STREAK).getValue());
        data.write("best_kill_streak_rank", getStat(StatType.BEST_KILL_STREAK).getRank());
    }

    @Override
    public void deserialize(SerializedData data) {
        this.uuid = data.applyAs("uuid", UUID.class);
        this.name = data.applyAs("name", String.class);


        Stat level = getStat(StatType.LEVEL);
        Stat kill = getStat(StatType.KILL);
        Stat death = getStat(StatType.DEATH);
        Stat experience = getStat(StatType.EXPERIENCE);
        Stat kill_streak = getStat(StatType.KILL_STREAK);
        Stat best_kill_streak = getStat(StatType.BEST_KILL_STREAK);

        int levelValue = data.applyAs("level", int.class) == null ? (int) StatType.LEVEL.getDefaultValue() : data.applyAs("level", int.class);
        int levelRank = data.applyAs("level_rank", int.class) == null ? 0 : data.applyAs("level_rank", int.class);
        int killsValue = data.applyAs("kills", int.class) == null ? (int) StatType.KILL.getDefaultValue() : data.applyAs("kills", int.class);
        int killsRank = data.applyAs("kills_rank", int.class) == null ? 0 : data.applyAs("kills_rank", int.class);
        int deathsValue = data.applyAs("deaths", int.class) == null ? (int) StatType.DEATH.getDefaultValue() : data.applyAs("deaths", int.class);
        int deathsRank = data.applyAs("deaths_rank", int.class) == null ? 0 : data.applyAs("deaths_rank", int.class);
        int expValue = data.applyAs("experience", int.class) == null ? (int)StatType.EXPERIENCE.getDefaultValue() : data.applyAs("experience", int.class);
        int expRank = data.applyAs("experience_rank", int.class) == null ? 0 : data.applyAs("experience_rank", int.class);
        int kill_s_value = data.applyAs("kill_streak", int.class) == null ? (int) StatType.KILL_STREAK.getDefaultValue() : data.applyAs("kill_streak", int.class);
        int kill_s_rank = data.applyAs("kill_streak_rank", int.class) == null ? 0 : data.applyAs("kill_streak_rank", int.class);
        int best_kill_s_value = data.applyAs("best_kill_streak", int.class) == null ? (int) StatType.BEST_KILL_STREAK.getDefaultValue() : data.applyAs("best_kill_streak", int.class);
        int best_kill_s_rank = data.applyAs("best_kill_streak_rank", int.class) == null ? 0 : data.applyAs("best_kill_streak_rank", int.class);

        level.setValue(levelValue);
        level.setRank(levelRank);

        kill.setValue(killsValue);
        kill.setRank(killsRank);

        death.setValue(deathsValue);
        death.setRank(deathsRank);

        experience.setValue(expValue);
        experience.setRank(expRank);

        kill_streak.setValue(kill_s_value);
        kill_streak.setRank(kill_s_rank);

        best_kill_streak.setValue(best_kill_s_value);
        best_kill_streak.setRank(best_kill_s_rank);
    }*/

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


/*    @Override
    public Object getDefaultDataValue(String columnName) {

        switch (columnName) {
            case "uuid":
            case "name":
            case "country":
            case "isActive":
            case "isActive2":
                return "";
            default:
                return 0;
        }
    }

    @Override
    public ArrayList<DataEntry<String, ColumnType>> getStructure() {
        ArrayList<DataEntry<String, ColumnType>> structure = new ArrayList<>();

        structure.add(new DataEntry<>("uuid", ColumnType.VARCHAR_UUID));
        structure.add(new DataEntry<>("name", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("phone", ColumnType.INTEGER));
        structure.add(new DataEntry<>("age", ColumnType.INTEGER));
        structure.add(new DataEntry<>("country", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("dValue", ColumnType.DOUBLE));
        structure.add(new DataEntry<>("isActive", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("isActive2", ColumnType.VARCHAR));
       // structure.add(new DataEntry<>("isActive3", ColumnType.VARCHAR));

        return structure;
    }

*//*
    @Override
    public void save(boolean async) {
        TestMain.testHold.saveSingle(this, async);
    }
*//*

    @Override
    public void serialize(SerializedData data) {
        data.write("uuid", uuid.toString());
        data.write("name", name);
        data.write("phone", phone);
        data.write("age", age);
        data.write("country", country);
        data.write("dValue", dValue);
        data.write("isActive", isActive);
        data.write("isActive2", isActive2);
        //data.write("isActive3", isActive3);
    }

    @Override
    public void deserialize(SerializedData data) {
        uuid = data.applyAs("uuid", UUID.class);
        name = data.applyAs("name", String.class);
        phone = data.applyAs("phone", int.class);
        age = data.applyAs("age", int.class);
        country = data.applyAs("country", String.class);
        dValue = data.applyAs("dValue", Double.class);
        isActive = data.applyAs("isActive", String.class);
        isActive2 = data.applyAs("isActive2", String.class);
        //isActive3 = data.applyAs("isActive3", String.class);
    }*/
}
