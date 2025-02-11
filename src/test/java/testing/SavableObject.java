package testing;

import io.aquaticlabs.aquaticdata.model.StorageModel;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

/**
 * @Author: extremesnow
 * On: 2/11/2025
 * At: 01:19
 */
@Getter @Setter
public class SavableObject implements StorageModel {

    private final UUID uuid;
    private int value;
    private boolean isEnabled;


    public SavableObject(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public Object getKey() {
        return uuid;
    }
}
