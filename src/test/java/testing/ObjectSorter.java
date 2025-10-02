package testing;

import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.util.DataEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @Author: extremesnow
 * On: 11/14/2021
 * At: 18:33
 */
public class ObjectSorter {

    private ObjectSorter(){}

    public static Map<UUID, DataEntry<SimpleStorageModel, Object>>sortInt(Map<UUID, DataEntry<SimpleStorageModel, Object>> items) {

        Map<UUID, DataEntry<SimpleStorageModel, Object>> convertedItems = new LinkedHashMap<>();
        for (Map.Entry<UUID, DataEntry<SimpleStorageModel, Object>> entry : items.entrySet()) {
            convertedItems.put(entry.getKey(), new DataEntry<>(entry.getValue().getKey(), entry.getValue().getValue()));
        }
        Map<UUID, DataEntry<SimpleStorageModel, Object>> sortedItems = new LinkedHashMap<>();
        Comparator<DataEntry<SimpleStorageModel, Object>> entryComparator = Comparator.comparingInt(o -> ((Integer) o.getValue()));

        convertedItems.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder(entryComparator)))
                .forEachOrdered(x -> sortedItems.put(x.getKey(), x.getValue()));

        return sortedItems;
    }

    public static Map<UUID, DataEntry<SimpleStorageModel, Object>> sortIntAndName(Map<UUID, DataEntry<SimpleStorageModel, Object>> items) {
        Map<UUID, DataEntry<SimpleStorageModel, Object>> convertedItems = new LinkedHashMap<>();
        for (Map.Entry<UUID, DataEntry<SimpleStorageModel, Object>> entry : items.entrySet()) {
            convertedItems.put(entry.getKey(), new DataEntry<>(entry.getValue().getKey(), entry.getValue().getValue()));
        }
        Map<UUID, DataEntry<SimpleStorageModel, Object>> sortedItems = new LinkedHashMap<>();

        Comparator<DataEntry<SimpleStorageModel, Object>> entryComparator = Comparator
                .comparingInt((DataEntry<SimpleStorageModel, Object> o) -> ((Integer) o.getValue()))
                .reversed()
                .thenComparing((DataEntry<SimpleStorageModel, Object> o) -> o.getKey().getValue("name").toString(), String.CASE_INSENSITIVE_ORDER);

        convertedItems.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(entryComparator))
                .forEachOrdered(x -> sortedItems.put(x.getKey(), x.getValue()));

        return sortedItems;
    }

    public static List<DataEntry<TestData, Object>> sortIntAndName(List<DataEntry<TestData, Object>> items) {
        List<DataEntry<TestData, Object>> convertedItems = new ArrayList<>(items);
        Comparator<DataEntry<TestData, Object>> entryComparator = Comparator
                .comparingInt((DataEntry<TestData, Object> o) -> ((Integer) o.getValue()))
                .reversed()
                .thenComparing((DataEntry<TestData, Object> o) -> o.getKey().getName(), String.CASE_INSENSITIVE_ORDER);

        return convertedItems.stream()
                .sorted(entryComparator)
                .collect(Collectors.toList());
    }

    public static Map<UUID, DataEntry<SimpleStorageModel, Object>> sortDouble(Map<UUID, DataEntry<SimpleStorageModel, Object>>items) {

        Map<UUID, DataEntry<SimpleStorageModel, Object>> convertedItems = new LinkedHashMap<>();
        for (Map.Entry<UUID, DataEntry<SimpleStorageModel, Object>> entry : items.entrySet()) {
            convertedItems.put(entry.getKey(), new DataEntry<>(entry.getValue().getKey(), entry.getValue().getValue()));
        }
        Map<UUID, DataEntry<SimpleStorageModel, Object>> sortedItems = new LinkedHashMap<>();
        Comparator<DataEntry<SimpleStorageModel, Object>> entryComparator = Comparator.comparingDouble(o -> ((Double) o.getValue()));

        convertedItems.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder(entryComparator)))
                .forEachOrdered(x -> sortedItems.put(x.getKey(), x.getValue()));

        return sortedItems;
    }

    public static Map<UUID, DataEntry<SimpleStorageModel, Object>> sortDoubleAndName(Map<UUID, DataEntry<SimpleStorageModel, Object>> items) {
        Map<UUID, DataEntry<SimpleStorageModel, Object>> convertedItems = new LinkedHashMap<>();
        for (Map.Entry<UUID, DataEntry<SimpleStorageModel, Object>> entry : items.entrySet()) {
            convertedItems.put(entry.getKey(), new DataEntry<>(entry.getValue().getKey(), entry.getValue().getValue()));
        }
        Map<UUID, DataEntry<SimpleStorageModel, Object>> sortedItems = new LinkedHashMap<>();

        Comparator<DataEntry<SimpleStorageModel, Object>> entryComparator = Comparator
                .comparingDouble((DataEntry<SimpleStorageModel, Object> o) -> ((Double) o.getValue()))
                .reversed()
                .thenComparing((DataEntry<SimpleStorageModel, Object> o) -> o.getKey().getValue("name").toString(), String.CASE_INSENSITIVE_ORDER);

        convertedItems.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(entryComparator))
                .forEachOrdered(x -> sortedItems.put(x.getKey(), x.getValue()));

        return sortedItems;
    }

}
