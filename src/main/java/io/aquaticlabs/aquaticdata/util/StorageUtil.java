package io.aquaticlabs.aquaticdata.util;

import com.google.gson.internal.Primitives;
import io.aquaticlabs.aquaticdata.data.object.SerializableObject;
import io.aquaticlabs.aquaticdata.data.storage.SerializedData;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
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


/*
    public static JsonElement primitiveToString(Object object) {
        if (object == null) return JsonNull.INSTANCE;
        Class clazz = Primitives.wrap(object.getClass());
        if (clazz == Integer.class) return new JsonPrimitive(object.toString());
        else if (clazz == Double.class) return new JsonPrimitive(object.toString());
        else if (clazz == Long.class) return new JsonPrimitive(object.toString());
        else if (clazz == Float.class) return new JsonPrimitive(object.toString());
        else if (clazz == Boolean.class)
            return new JsonPrimitive((((Boolean) object) ? "1" : "0"));
        else if (clazz == String.class) return new JsonPrimitive(object.toString());
        else
            throw new IllegalStateException(
                    "Failed to serialize object of type " + object.getClass().getSimpleName());
    }
*/

/*    public static JsonElement wrap(Object object) {
        if (object == null) {
            return JsonNull.INSTANCE;
        }

        // If primitive just convert to string
        if (isPrimitive(object.getClass())) {
            return primitiveToString(object);
        }

        // If implements SerializableObject, serialize
        if (object instanceof SerializableObject) {
            SerializedData serializedData = new SerializedData();
            ((SerializableObject) object).serialize(serializedData);

            return serializedData.getJsonElement();
        }

        // If is collection
        if (object instanceof Collection) {
            JsonArray array = new JsonArray();
            for (Object listObject : (Collection) object) array.add(wrap(listObject));

            return array;
        }

        // If is map
        if (object instanceof Map) {
            Map map = (Map) object;
            JsonArray array = new JsonArray();
            map.forEach(
                    (key, value) -> {
                        JsonObject jsonObject = new JsonObject();
                        jsonObject.add("key", wrap(key));
                        jsonObject.add("value", wrap(value));
                        array.add(jsonObject);
                    });
            return array;
        }

        // If it's an enum
        if (object.getClass().isEnum()) {
            return new JsonPrimitive(((Enum) object).name());
        }

        return AquaticDatabase.getInstance().getGson().toJsonTree(object);
    }*/

    public static String fromObject(Object object) {
        if (!(object instanceof String)) {
            if (object instanceof Boolean) {
                object = (boolean)object ? 1 : 0;
            }
            return object.toString();
        }

        try {
            String objectString = (String) object;
            objectString = objectString.replace("\\", "");
            objectString = objectString.replace("\"", "");
            return objectString;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return object.toString();
    }

    public static <T> T fromObject(Object object, Class<T> clazz) {
        clazz = Primitives.wrap(clazz);

        if (SerializableObject.class.isAssignableFrom(clazz))
            return fromSerializable(object, clazz);

        if (clazz.isEnum()) return (T) Enum.valueOf((Class<Enum>) clazz, object.toString());

        if (object instanceof Boolean) {

        }

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


    @SneakyThrows
    public static <T> T fromSerializable(Object value, Class<T> clazz) {
        T object = (T) getConstructor(clazz).newInstance();
        ((SerializableObject) object).deserialize(new SerializedData());
        return object;
    }

    @SneakyThrows
    public static Constructor<?> getConstructor(Class<?> clazz) {
        Constructor<?> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor;
    }


}