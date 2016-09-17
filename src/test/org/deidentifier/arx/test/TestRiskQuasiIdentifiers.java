/*
 * ARX: Powerful Data Anonymization
 * Copyright 2012 - 2016 Fabian Prasser, Florian Kohlmayer and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.deidentifier.arx.test;

import org.apache.commons.lang.StringUtils;
import org.deidentifier.arx.*;
import org.deidentifier.arx.risk.RiskEstimateBuilder;
import org.deidentifier.arx.risk.RiskModelAttributes;
import org.deidentifier.arx.risk.RiskModelQI;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

/**
 * Test for QuasiIdentifiers
 *
 * @author Max Zitzmann
 */
public class TestRiskQuasiIdentifiers {

    @Test
    public void testWithDefinedDataSet() {
        // Define data
        Data.DefaultData data = Data.create();
        data.add("age", "sex", "state");
        data.add("20", "Female", "CA");
        data.add("30", "Female", "CA");
        data.add("40", "Female", "TX");
        data.add("20", "Male", "NY");
        data.add("40", "Male", "CA");

        // calculated by hand
        ResultSet[] expectedResults = new ResultSet[]{
                new ResultSet("[sex]", 0.4, 0.6),
                new ResultSet("[state]", 0.6, 0.7),
                new ResultSet("[age]", 0.6, 0.8),
                new ResultSet("[sex, state]", 0.8, 0.9),
                new ResultSet("[age, state]", 1.0, 1.0),
                new ResultSet("[sex, age]", 1.0, 1.0),
                new ResultSet("[sex, age, state]", 1.0, 1.0),
        };

        // flag every identifier as quasi identifier
        for (int i = 0; i < data.getHandle().getNumColumns(); i++) {
            data.getDefinition().setAttributeType(data.getHandle().getAttributeName(i), AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        }

        // perform calculation
        RiskEstimateBuilder builder = data.getHandle().getRiskEstimator(null);
        RiskModelAttributes riskmodel = builder.getAttributeRisks(true);
        RiskModelAttributes.QuasiIdentifierRisk risks[] = riskmodel.getAttributeRisks();

        for (int i = 0; i < risks.length; i++) {
            assertTrue("Identifier expected: " + expectedResults[i].identifier + "; got: " + risks[i].getIdentifier(), expectedResults[i].identifier.equals(risks[i].getIdentifier().toString()));
            assertTrue("Distinction expected: " + expectedResults[i].calculatedDistinction + "; got: " + risks[i].getDistinction(), expectedResults[i].calculatedDistinction == risks[i].getDistinction());
            assertTrue("Separation expected: " + expectedResults[i].calculatedSeparation + "; got: " + risks[i].getSeparation(), expectedResults[i].calculatedSeparation == risks[i].getSeparation());
        }
    }

    @Test
    public void compareRuntime() {
        boolean newMethod = true;
        Data data = loadCsv(30000);
        //data = loadCsvSkriptData();

        assertTrue(data != null);

        // flag every identifier as quasi identifier
        for (int i = 0; i < data.getHandle().getNumColumns(); i++) {
            data.getDefinition().setAttributeType(data.getHandle().getAttributeName(i), AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        }
        RiskModelAttributes.QuasiIdentifierRisk[] risks;
        long startTimeNew = System.currentTimeMillis();
        risks = calculateAttributeRisks(data, newMethod);
        long endTimeNew = System.currentTimeMillis();

        //printPrettyTable(risks);
        System.out.println("Method type:\t" + (newMethod ? "NEW" : "OLD"));
        System.out.println("Execution Time:\t" + (endTimeNew - startTimeNew));
    }

    private RiskModelAttributes.QuasiIdentifierRisk[] calculateAttributeRisks(Data data, boolean newMethod) {
        RiskEstimateBuilder builder = data.getHandle().getRiskEstimator(null);
        RiskModelAttributes riskModel = builder.getAttributeRisks(newMethod);
        return riskModel.getAttributeRisks();
    }

    @Test
    public void compareWithOldMethod() {
        DataProvider dataProvider = new DataProvider();
        dataProvider.createDataDefinition();
        Data data = dataProvider.getData();

        RiskEstimateBuilder builder = data.getHandle().getRiskEstimator(null);
        RiskModelAttributes riskModel = builder.getAttributeRisks(true);
        RiskModelAttributes.QuasiIdentifierRisk risksNewMethod[] = riskModel.getAttributeRisks();


        for (RiskModelAttributes.QuasiIdentifierRisk riskNewMethod : risksNewMethod) {
            RiskModelQI riskModelQI = new RiskModelQI(data.getHandle(), riskNewMethod.getIdentifier());
            double newValue = truncate(riskNewMethod.getDistinction(), 7);
            double oldValue = truncate(riskModelQI.getAlphaDistinct(), 7);
            assertTrue("Mismatch: \nNewMethod:\t" + newValue + "\nOldMethod:\t" + oldValue, oldValue == newValue);
        }
    }

    private Data loadCsv(int countRecords) {
        Data data = null;
        try {
            data = loadCsvExample("data/example_" + countRecords + ".csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    private static Data loadCsvExample(String csvFile) throws IOException {
        System.out.println("- Loading CSV file " + csvFile);
        DataSource source = DataSource.createCSVSource(csvFile, StandardCharsets.UTF_8, ';', true);
        source.addColumn("sex", DataType.STRING);
        source.addColumn("age", DataType.INTEGER);
        source.addColumn("race", DataType.STRING);
        source.addColumn("marital-status", DataType.STRING);
        source.addColumn("education", DataType.STRING);
        source.addColumn("native-country", DataType.STRING);
        source.addColumn("workclass", DataType.STRING);
        source.addColumn("occupation", DataType.STRING);
        source.addColumn("salary-class", DataType.STRING);
        source.addColumn("ldl", DataType.STRING);

        return Data.create(source);
    }

    private static Data loadCsvSkriptData() throws IOException {
        String csvFile = "data/skript_data.csv";
        DataSource source = DataSource.createCSVSource(csvFile, StandardCharsets.UTF_8, ';', true);
        source.addColumn("age", DataType.INTEGER);
        source.addColumn("sex", DataType.STRING);
        source.addColumn("state", DataType.STRING);
        return Data.create(source);
    }


//    private static Data loadCsvExample2() throws IOException {
//        String csvFile = "data/adult.csv";
//
//        DataSource source = DataSource.createCSVSource(csvFile, StandardCharsets.UTF_8, ';', true);
//        source.addColumn("sex", DataType.STRING);
//        source.addColumn("age", DataType.INTEGER);
//        source.addColumn("race", DataType.STRING);
//        source.addColumn("marital-status", DataType.STRING);
//        source.addColumn("education", DataType.STRING);
//        source.addColumn("native-country", DataType.STRING);
//        source.addColumn("workclass", DataType.STRING);
//        source.addColumn("occupation", DataType.STRING);
//        source.addColumn("salary-class", DataType.STRING);
//        source.addColumn("ldl", DataType.STRING);
//
//        return Data.create(source);
//    }

    private static double truncate(double value, int places) {
        double multiplier = Math.pow(10, places);
        return Math.floor(multiplier * value) / multiplier;
    }

    private class ResultSet {
        String identifier;
        double calculatedDistinction;
        double calculatedSeparation;

        ResultSet(String identifier, double calculatedDistinction, double calculatedSeparation) {
            this.identifier = identifier;
            this.calculatedDistinction = calculatedDistinction;
            this.calculatedSeparation = calculatedSeparation;
        }

    }

    private static void printPrettyTable(RiskModelAttributes.QuasiIdentifierRisk[] quasiIdentifiers) {
        // get char count of longest quasi-identifier
        ;
        int charCountLongestQi = quasiIdentifiers[quasiIdentifiers.length - 1].getIdentifier().toString().length();

        // make sure that there is enough space for the table header strings
        charCountLongestQi = Math.max(charCountLongestQi, 12);

        // calculate space needed
        String leftAlignFormat = "| %-" + charCountLongestQi + "s | %13.2f | %12.2f |%n";

        // add 2 spaces that are in the string above on the left and right side of the first pattern
        charCountLongestQi += 2;

        // subtract the char count of the column header string to calculate
        // how many spaces we need for filling up to the right columnborder
        int spacesAfterColumHeader = charCountLongestQi - 12;

        System.out.format("+" + StringUtils.repeat("-", charCountLongestQi) + "+---------------+--------------+%n");
        System.out.format("| Identifier " + StringUtils.repeat(" ", spacesAfterColumHeader) + "| α-Distinction | α-Separation |%n");
        System.out.format("+" + StringUtils.repeat("-", charCountLongestQi) + "+---------------+--------------+%n");
        for (RiskModelAttributes.QuasiIdentifierRisk quasiIdentifier : quasiIdentifiers) {
            // print every Quasi-Identifier
            System.out.format(leftAlignFormat, quasiIdentifier.getIdentifier(), quasiIdentifier.getDistinction() * 100, quasiIdentifier.getSeparation() * 100);
        }
        System.out.format("+" + StringUtils.repeat("-", charCountLongestQi) + "+---------------+--------------+%n");
    }
}
