package testing.newtest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @Author: extremesnow
 * On: 4/26/2023
 * At: 18:52
 */
@Getter @Setter
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

    private boolean isSurfaceOre = false;

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

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
             OutputStreamWriter writer = new OutputStreamWriter(gzipOut, "UTF-8")) {

            writer.write(jsonString);
            writer.flush();
            gzipOut.finish();

            return baos.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    // Decompress and deserialize byte array back to a CheckData object
    public static CheckData decompressJsonFromBytes(byte[] compressedBytes) {
        if (compressedBytes == null || compressedBytes.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
             GZIPInputStream gzipIn = new GZIPInputStream(bais);
             InputStreamReader reader = new InputStreamReader(gzipIn, "UTF-8");
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            StringBuilder jsonBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                jsonBuilder.append(line);
            }

            // Use GsonBuilder to allow for missing fields in JSON
            Gson gson = new GsonBuilder()
                    .serializeNulls()  // Handle null values explicitly
                    .create();

            return gson.fromJson(jsonBuilder.toString(), CheckData.class);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        return "CheckData{" +
                "playerUUID=" + playerUUID +
                ", value=" + value +
                ", veinSize=" + veinSize +
                ", threshold=" + threshold +
                ", averageValue=" + averageValue +
                ", distance=" + distance +
                ", distanceExpected=" + distanceExpected +
                ", materialName='" + materialName + '\'' +
                ", locationString='" + locationString + '\'' +
                ", lightLevel=" + lightLevel +
                ", timestamp=" + timestamp +
                ", timeValue=" + timeValue +
                ", isSurfaceOre=" + isSurfaceOre +
                '}';
    }
    /*    public byte[] compressJsonToBytes() {
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
    }*/
}
