package io.aquaticlabs.aquaticdata.util;

/**
 * @Author: extremesnow
 * On: 8/22/2022
 * At: 18:19
 */
public class ConstuctorFailThrowable extends Exception {

    public ConstuctorFailThrowable() { super(); }
    public ConstuctorFailThrowable(String message) { super(message); }
    public ConstuctorFailThrowable(String message, Throwable cause) { super(message, cause); }
    public ConstuctorFailThrowable(Throwable cause) { super(cause); }

}
