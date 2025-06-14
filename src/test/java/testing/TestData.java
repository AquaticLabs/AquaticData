package testing;

import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.Getter;
import lombok.Setter;

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
    private int value;
    private int value2 = 5;


    public TestData() {
    }

    public TestData(UUID uuid) {
        this.key = uuid;
    }

    @Override
    public UUID getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "TestData{" +
                "key=" + key +
                ", name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
