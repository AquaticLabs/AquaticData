package testing;

import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: extremesnow
 * On: 1/6/2025
 * At: 02:17
 */
public class Leaderboard {

    @Getter
    private final SimpleStatType statType;
    private final List<DataEntry<SimpleStorageModel, Object>> dataList = new ArrayList<>();

    @Getter
    @Setter
    private boolean statsChanged = false;

    @Getter
    private final int leaderboardSize;

    public Leaderboard(SimpleStatType statType, int leaderboardSize) {
        this.statType = statType;
        this.leaderboardSize = leaderboardSize;
    }


    public void updateTop(List<DataEntry<SimpleStorageModel, Object>> modelList) {
        System.out.println("update top 1: " + dataList.size());

        dataList.clear();
        dataList.addAll(modelList);
        System.out.println("update top: " + dataList.size());

    }

    public Map<SimpleStorageModel, Object> getTop(int amount) {
        Map<SimpleStorageModel, Object> topAmount = new LinkedHashMap<>();
        int i = 0;
        for (DataEntry<SimpleStorageModel, Object> entry : dataList) {
            if (i == amount) break;
            topAmount.put(entry.getKey(), entry.getValue());
            i++;
        }
        return topAmount;
    }
}
