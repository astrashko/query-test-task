package org.query.calc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryCalcImpl implements QueryCalc {
    private static final int LIMIT = 10;

    private static final int RANGE_START = 0;
    private static final int RANGE_END = 1_000_000;

    private final Function<? super Map.Entry<Double, Double>, ? extends double[]> mapEntryToArray = entry -> new double[]{entry.getKey(), entry.getValue()};
    private final Collector<double[], ?, LinkedHashMap<Double, Double>> groupByKeySumByValueKeepOrder = Collectors.groupingBy(doubles -> doubles[0], LinkedHashMap::new, Collectors.summingDouble(value -> value[1]));
    private final Collector<double[], ?, Map<Double, Double>> groupByKeySumByValue = Collectors.groupingBy(doubles -> doubles[0], HashMap::new, Collectors.summingDouble(value -> value[1]));

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        double[][] t2t3 = crossJoinTables(t2, t3);
        t2t3 = groupByBPlusCSumYZ(t2t3);

        double[][] ax = filePathToArray(t1);

        double[][] leftJoin = leftJoin(t2t3, ax);

        double[][] result = limitResult(leftJoin);

        printResult(result, output);
    }

    //a, x
    private double[][] filePathToArray(Path path) throws IOException {
        readFirstLineAndValidate(path);

        return Files.readAllLines(path)
                .stream()
                .skip(1)
                .map(this::fileLineToArray)
                .collect(groupByKeySumByValueKeepOrder)
                .entrySet().stream()
                .map(mapEntryToArray)
                .toArray(double[][]::new);
    }

    //b+c, y*z
    private double[][] crossJoinTables(Path a, Path b) throws IOException {
        int sizeA = readFirstLineAndValidate(a);
        int sizeB = readFirstLineAndValidate(b);

        List<String> listA = Files.readAllLines(a);
        List<String> listB = Files.readAllLines(b);

        double[][] result = new double[sizeA * sizeB][2];

        int k = 0;
        for (int i = 1; i < listA.size(); i++) {
            for (int j = 1; j < listB.size(); j++) {
                double[] array1 = fileLineToArray(listA.get(i));
                double[] array2 = fileLineToArray(listB.get(j));
                double[] row = new double[array1.length];
                row[0] = array1[0] + array2[0];
                row[1] = array1[1] * array2[1];

                result[k++] = row;
            }
        }

        return result;
    }

    private double[] fileLineToArray(String s) {
        return Arrays.stream(s.split(" ")).map(Double::parseDouble).mapToDouble(value -> value).toArray();
    }

    //group by b+c, sum(y*z) and sort by b+c
    private double[][] groupByBPlusCSumYZ(double[][] table) {
        return Arrays.stream(table)
                .collect(groupByKeySumByValue)
                .entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(mapEntryToArray)
                .toArray(double[][]::new);
    }

    private int findNextGreater(double[][] array, double element) {
        int startIndex = 0;
        int endIndex = array.length - 1;

        int result = -1;
        while (startIndex <= endIndex) {
            int middle = (startIndex + endIndex) / 2;

            if (array[middle][0] <= element) {
                startIndex = middle + 1;
            } else {
                result = middle;
                endIndex = middle - 1;
            }
        }

        return result;
    }

    private double[][] leftJoin(double[][] t2t3, double[][] ax) {
        double[][] result = new double[ax.length][2];
        for (int i = 0; i < ax.length; i++) {
            int nextGreaterIndex = findNextGreater(t2t3, ax[i][0]);
            double sumXYZ = 0;
            if (nextGreaterIndex == -1) {
                result[i][0] = ax[i][0];
                result[i][1] = 0;
                continue;
            }
            for (int j = nextGreaterIndex; j < t2t3.length; j++) {
                sumXYZ += t2t3[j][1] * ax[i][1];
            }
            result[i][0] = ax[i][0];
            result[i][1] = sumXYZ;
        }

        return result;
    }

    private double[][] limitResult(double[][] table) {
        return Arrays.stream(table)
                .collect(groupByKeySumByValue)
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.<Double, Double>comparingByValue()).thenComparing(Map.Entry.comparingByKey()))
                .limit(LIMIT)
                .map(mapEntryToArray)
                .toArray(double[][]::new);
    }

    private void printResult(double[][] table, Path path) throws IOException {
        String result = Stream.concat(
                Stream.of(table.length + ""),
                Arrays.stream(table).map(doubles -> Arrays.stream(doubles).boxed().map(x -> x % 1 == 0 ? String.format("%.0f", x) : String.format("%.6f", x)).collect(Collectors.joining(" ")))
        ).collect(Collectors.joining(System.lineSeparator()));

        Files.write(path, result.getBytes());
    }

    private int readFirstLineAndValidate(Path path) throws IOException {
        BufferedReader readerA = Files.newBufferedReader(path);
        int size = Integer.parseInt(readerA.readLine());

        if (!(size > RANGE_START && size <= RANGE_END)) {
            throw new IllegalArgumentException("Number of rows of table should lay in range [0, 1_000_000].");
        }

        return size;
    }
}
