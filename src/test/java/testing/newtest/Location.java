package testing.newtest;

import lombok.Getter;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 14:47
 */
@Getter
public class Location {

    private String world;
    private double x;
    private double y;
    private double z;


    public Location(String world, double x, double y, double z) {
        this.world = world;
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public String getLocationString() {
        return world + ", " + x + ", " + y + ", " + z;

    }



}
