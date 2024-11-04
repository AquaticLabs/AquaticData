package testing.newtest;

import com.google.gson.Gson;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @Author: extremesnow
 * On: 4/26/2023
 * At: 18:52
 */
@Getter
public class CheckData implements Serializable {

    private final UUID playerUUID;
    private int value;
    private int veinSize;
    private int threshold;
    private int averageValue;
    private int distance;
    private int distanceExpected;
    private final String materialName;
    private final String locationString;
    private final byte lightLevel;
    private final long timestamp;
    private long timeValue;

    public CheckData(UUID playerUUID, int value, Material material, Location location, byte lightLevel, long timestamp) {
        this.playerUUID = playerUUID;
        this.value = value;
        this.materialName = material != null ? material.name() : "None";
        this.locationString = location != null ? location.getLocationString() : "";
        this.lightLevel = lightLevel;
        this.timestamp = timestamp;
    }

    public byte[] compressJsonToBytes() {
        Gson gson = new Gson();
        String jsonString = gson.toJson(this);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(jsonString.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    public static CheckData decompressJsonFromBytes(byte[] compressedBytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
        try (GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            StringBuilder jsonBuilder = new StringBuilder();
            while ((bytesRead = gzipIn.read(buffer)) != -1) {
                jsonBuilder.append(new String(buffer, 0, bytesRead));
            }

            Gson gson = new Gson();
            return gson.fromJson(jsonBuilder.toString(), CheckData.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
