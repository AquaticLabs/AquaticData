package io.aquaticlabs.aquaticdata.util;

import com.google.common.primitives.Primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StorageUtil {


    private static final List<Class> primitiveList =
            new ArrayList<Class>() {
                {
                    add(Integer.class);
                    add(String.class);
                    add(Double.class);
                    add(Long.class);
                    add(Boolean.class);
                    add(Float.class);
                }
            };

    public static boolean isPrimitive(Class clazz) {
        clazz = Primitives.wrap(clazz);
        return primitiveList.contains(clazz);
    }


    public static boolean isAtDefaultValue(Object object) {
        if (object instanceof Number) {
            return ((Number) object).intValue() == 0;
        }
        return object == null;
    }

    public static void calculateMoves(Map<String, String> movesNeeded, List<String> order, List<String> desiredOrder) {
        List<String> fixing = new ArrayList<>(order);
        int maxIterator = 0;
        while (!isIdentical(fixing, desiredOrder) && maxIterator <= 10) {
            changeOrder(movesNeeded, fixing, desiredOrder);
            maxIterator++;
        }
    }

    public static boolean isIdentical(List<String> list1, List<String> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!list1.get(i).equals(list2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void changeOrder(Map<String, String> movesNeeded, List<String> fixing, List<String> desiredOrder) {
        List<String> fixingClone = new ArrayList<>(fixing);
        int invalidPos = -1;

        int i = 0;
        for (String value : fixingClone) {
            String desiredVal = desiredOrder.get(i);
            if (!desiredVal.equals(value) && invalidPos == -1) {
                invalidPos = i++;
                continue;
            }
            if (invalidPos == -1) {
                i++;
                continue;
            }

            String monitoredVal = desiredOrder.get(invalidPos);

            if (monitoredVal.equals(value)) {
                movesNeeded.put(value, desiredOrder.get(invalidPos - 1));

                fixing.remove(value);
                fixing.add(invalidPos, value);
            }
            i++;
        }
    }

    public static String fromObject(Object object) {
        if (!(object instanceof String)) {
            if (object instanceof Boolean) {
                object = (boolean) object ? 1 : 0;
            }
            return object.toString();
        }

        try {
            String objectString = (String) object;
            objectString = objectString.replace("\\", "");
            objectString = objectString.replace("\"", "");
            return objectString;

        } catch (Exception e) {
            DataDebugLog.logError("Failed to create object from string: ", e);
        }
        return object.toString();
    }

    public static <T> T fromObject(Object object, Class<T> clazz) {
        clazz = Primitives.wrap(clazz);

        if (clazz.isEnum()) return (T) Enum.valueOf((Class<Enum>) clazz, object.toString());


        Object parsed = fromObject(object);

        if (clazz != parsed.getClass()) {

            parsed = doConversion(parsed, clazz);
        }

        return (T) parsed;
    }

    private static <T> Object doConversion(Object parsed, Class<T> clazz) {
        String value = parsed.toString();
        value = value.replace("\\", "");
        value = value.replace("\"", "");

        if (clazz == String.class) return value;


        if (clazz == UUID.class) return UUID.fromString(value);

        clazz = Primitives.wrap(clazz);
        if ((clazz == Double.class || clazz == Float.class)
                && Primitives.wrap(clazz) == Integer.class) {
            if (value.contains(".")) {
                String[] split = value.split("\\.");
                int noZeroCounter = 0;
                for (char c : split[1].toCharArray()) if (c != '0') noZeroCounter++;

                if (noZeroCounter == 0) value = split[0];
            }
        }

        if (clazz == Integer.class) return Integer.parseInt(value);
        else if (clazz == Boolean.class) return value.toCharArray()[0] != '0';
        else if (clazz == Long.class) return Long.parseLong(value);
        else if (clazz == Float.class) return Float.parseFloat(value);
        else if (clazz == Double.class) return Double.parseDouble(value);
        else
            throw new IllegalStateException(
                    "Failed to convert "
                            + parsed.getClass().getSimpleName()
                            + " to "
                            + clazz.getSimpleName()
                            + " unknown conversion!");
    }
}
