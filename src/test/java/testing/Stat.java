package testing;

import lombok.Getter;
import lombok.Setter;

/**
 * @Author: extremesnow
 * On: 10/13/2021
 * At: 23:42
 */
public class Stat {

    @Getter
    private final StatType type;
    @Getter @Setter
    private Object value = 0;
    @Getter @Setter
    private int rank = 0;

    public Stat(StatType type) {
        this.type = type;
    }

    public Stat(StatType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Stat(StatType type, Object value, int rank) {
        this.type = type;
        this.value = value;
        this.rank = rank;
    }

    public void addNumberValue(Object amount) {
        if (type.getClassType() == int.class) {
            int oldVal = (int) value;
            int amToAdd = (int) amount;
            setValue(oldVal + amToAdd);
            return;
        }
        double oldVal = ((Number) value).doubleValue();
        double amToAdd = ((Number) amount).doubleValue();

        setValue(oldVal + amToAdd);
    }

    public void removeNumberValue(Object amount) {
        if (type.getClassType() == int.class) {
            int oldVal = (int) value;
            int amToRem = (int) amount;
            int updatedVal = Math.max(oldVal - amToRem, 0);
            setValue(updatedVal);
            return;
        }
        double oldVal = ((Number) value).doubleValue();
        double amToRem = ((Number) amount).doubleValue();
        double updatedVal = Math.max(oldVal - amToRem, 0);
        setValue(updatedVal);

    }

}
