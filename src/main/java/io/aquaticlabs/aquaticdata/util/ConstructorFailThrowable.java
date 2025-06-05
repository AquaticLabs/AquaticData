package io.aquaticlabs.aquaticdata.util;

/**
 * @Author: extremesnow
 * On: 8/22/2022
 * At: 18:19
 */
public class ConstructorFailThrowable extends Exception {

    public ConstructorFailThrowable() { super(); }
    public ConstructorFailThrowable(String message) { super(message); }
    public ConstructorFailThrowable(String message, Throwable cause) { super(message, cause); }
    public ConstructorFailThrowable(Throwable cause) { super(cause); }

}
