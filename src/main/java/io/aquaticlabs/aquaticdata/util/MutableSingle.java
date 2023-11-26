package io.aquaticlabs.aquaticdata.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class MutableSingle<T> {
    private T value;
    private T previousValue;
    private final List<Consumer<T>> listeners;
    private final OperatorChain<T> operatorChain;


    public MutableSingle() {
        // initialize the value field to null
        value = null;
        previousValue = null;
        listeners = new ArrayList<>();
        operatorChain = new OperatorChain<>(Function.identity()); // Default identity function
    }
    public MutableSingle(T value) {
        // initialize the value field to value
        this.value = value;
        this.previousValue = null;
        this.listeners = new ArrayList<>();
        operatorChain = new OperatorChain<>(Function.identity()); // Default identity function
    }

    public T get() {
        return value;
    }

    public T getOperated() {
        T result = value;
        result = operatorChain.apply(result);
        return result;
    }

    public void set(T newValue) {
        if (!newValue.equals(previousValue)) {
            value = newValue;
            previousValue = newValue;
            for (Consumer<T> listener : listeners) {
                listener.accept(newValue);
            }
        }
    }

    public void onChanged(Consumer<T> listener) {
        listeners.add(listener);
    }


    public void addOperator(Function<T, T> operator) {
        operatorChain.addOperator(operator);
    }


    private static class OperatorChain<T> {
        private final List<Function<T, T>> operators;

        public OperatorChain(Function<T, T> identity) {
            operators = new ArrayList<>();
            operators.add(identity);
        }

        public void addOperator(Function<T, T> operator) {
            operators.add(operator);
        }

        public T apply(T value) {
            for (Function<T, T> operator : operators) {
                value = operator.apply(value);
            }
            return value;
        }
    }

}