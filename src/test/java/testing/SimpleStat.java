package testing;

import io.aquaticlabs.aquaticdata.util.MutableSingle;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author: extremesnow
 * On: 10/13/2021
 * At: 23:42
 */
public class SimpleStat {

    @Getter
    private final SimpleStatType type;
    @Getter
    @Setter
    private Object value = 0;
    @Getter
    private MutableSingle<Integer> rank = new MutableSingle<>(0);

    public SimpleStat(SimpleStatType type) {
        this.type = type;
    }

    public SimpleStat(SimpleStatType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public SimpleStat(SimpleStatType type, Object value, int rank) {
        this.type = type;
        this.value = value;
        this.rank.set(rank);
    }

    public boolean setRank(int rankNum) {
        return this.rank.set(rankNum);
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
