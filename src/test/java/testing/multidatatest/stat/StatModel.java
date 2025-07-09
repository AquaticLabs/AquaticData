package testing.multidatatest.stat;

import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class StatModel implements StorageModel {

    private UUID uuidKey;
    private String name;
    private int value;

    @Override
    public Object getKey() {
        return uuidKey;
    }
}
