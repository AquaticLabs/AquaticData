package testing;


import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OrderMatcher {

    public static void main(String[] args) {

        StringBuilder builder = new StringBuilder();

        builder.append("Test, ");

        builder.deleteCharAt(builder.toString().length() - 2);
        System.out.println(builder);


        ArrayList<String> desiredOrder = new ArrayList<>(Arrays.asList("UUID", "Name", "val1", "val2", "val3", "val4", "val5"));


        System.out.println(desiredOrder);


        System.out.println("--- Moves Calculation ---");
        System.out.println("|  Test #  | Swap Method |  Move Method |");

        List<ArrayList<String>> shuffles = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            ArrayList<String> orderShuffle = new ArrayList<>(Arrays.asList("Name", "val1", "val2", "val3", "val4", "val5"));
            Collections.shuffle(orderShuffle);
            orderShuffle.add(0, "UUID");
            shuffles.add(orderShuffle);
        }

        for (int i = 1; i < 10; i++) {
            Map<String, String> movesNeededSwap = calculateMoves(shuffles.get(i-1), desiredOrder, true);
            Map<String, String> movesNeededMove = calculateMoves2(shuffles.get(i-1), desiredOrder, true);
            System.out.println("|     " + i + "    |      " + movesNeededSwap.size() + "      |       " + movesNeededMove.size() + "      |");
        }

        System.out.println("-----------------------------------------");

        int i = 1;
        for (ArrayList<String> shuffle : shuffles) {
            System.out.println("Shuffle # " + i++ + " : " + shuffle);

        }


    }


    public static Map<String, String> calculateMoves(List<String> order, List<String> desiredOrder, boolean sub) {
        Map<String, String> movesNeeded = new LinkedHashMap<>();

        List<String> fixing = new ArrayList<>(order);

        int maxIterator = 0;
        while (!isIdentical(fixing, desiredOrder) && maxIterator <= 10) {
            // System.out.println("FixWhile: " + fixing);
            changeOrder(movesNeeded, fixing, desiredOrder, sub);
            maxIterator++;
        }

        return movesNeeded;
    }

    public static Map<String, String> calculateMoves2(List<String> order, List<String> desiredOrder, boolean sub) {
        Map<String, String> movesNeeded = new LinkedHashMap<>();

        List<String> fixing = new ArrayList<>(order);

        int maxIterator = 0;
        while (!isIdentical(fixing, desiredOrder) && maxIterator <= 10) {
            //   System.out.println("FixWhile: " + fixing);
            changeOrder2(movesNeeded, fixing, desiredOrder, sub);
            maxIterator++;
        }

        return movesNeeded;
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

    public static void changeOrder(Map<String, String> movesNeeded, List<String> fixing, List<String> desiredOrder, boolean sub) {
        List<String> desiredClone = new ArrayList<>(desiredOrder);

        int invalidPos = -1;
        int desiredPos = -1;

        int i = 0;
        for (String string : desiredClone) {
            String fixVal = fixing.get(i);
            if (!string.equals(fixVal) && invalidPos == -1) {
                //System.out.println("found fail at: " + fixVal + " invalidPos: " + i);
                invalidPos = i++;
                continue;
            }

            if (invalidPos != -1) {
                String invalidVal = fixing.get(invalidPos);
                // System.out.println("checking NextVal: " + i + " invalid entry: " + invalidVal);

                if (string.equals(invalidVal)) {
                    desiredPos = i;
                    Collections.swap(fixing, invalidPos, desiredPos);

                    String orderV = desiredOrder.get(i);
                    if (sub) {
                        orderV = desiredOrder.get(i - 1);
                    }
                    movesNeeded.put(invalidVal, orderV);


                    break;
                }
            }
            i++;
        }
    }

    public static void changeOrder2(Map<String, String> movesNeeded, List<String> fixing, List<String> desiredOrder, boolean sub) {
        List<String> fixingClone = new ArrayList<>(fixing);
        int invalidPos = -1;

        int i = 0;
        for (String value : fixingClone) {
            String desiredVal = desiredOrder.get(i);
            if (!desiredVal.equals(value) && invalidPos == -1) {
                //System.out.println("found fail at: " + value + " invalidPos: " + i);
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

}