package testing;

import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.Getter;
import lombok.Setter;

import java.util.EnumMap;
import java.util.UUID;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 23:55
 */
@Getter @Setter
public class TestData implements StorageModel {

    private UUID key;
    private String name;
    private int value2 = 5;

    private EnumMap<SimpleStatType, SimpleStat> statMap;


    public TestData() {
        populateStatMap();
    }

    public TestData(UUID uuid) {
        this.key = uuid;
        populateStatMap();
    }
    public SimpleStat getStat(SimpleStatType type) {
        return statMap.get(type);
    }
    @Override
    public UUID getKey() {
        return key;
    }

    private void populateStatMap() {
        if (statMap == null) {
            statMap = new EnumMap<>(SimpleStatType.class);
        }
        for (SimpleStatType type : SimpleStatType.values()) {
            statMap.computeIfAbsent(type, k -> new SimpleStat(type, type.getDefaultValue()));
        }
    }

    @Override
    public String toString() {
        return "TestData{" +
                "key=" + key +
                ", name='" + name + '\'' +
                ", value2=" + value2 +
                '}';
    }
}
