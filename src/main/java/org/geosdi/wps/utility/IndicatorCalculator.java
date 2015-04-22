/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps.utility;

import eu.crismaproject.icmm.icmmhelper.pilotD.Indicators;
import eu.crismaproject.icmm.icmmhelper.pilotD.PilotDHelper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class IndicatorCalculator {

    private IndicatorCalculator() {
    }

    //Lost builings formula: from table building damage sum the total from columns nd4+nd5
    //SELECT sum(nd4 + nd5) FROM ws_16.building_damage;
    private static Integer calculateLostBuildings(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(nd4 + nd5) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer lostBuildings = 0;
        while (resultSet.next()) {
            lostBuildings = Math.round(resultSet.getFloat(1));
        }
        return lostBuildings;
    }

    //Unsafe Buildings formula: from table building damage sum the total from columns ((0.5 * nd3) + nd4 + nd5)
    //SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ws_1.building_damage;
    private static Integer calculateUnsafeBuildings(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer unsafeBuildings = 0;
        while (resultSet.next()) {
            unsafeBuildings = Math.round(resultSet.getFloat(1));
        }
        return unsafeBuildings;
    }

    //No Of Dead formula: from table casualties sum each row from column deads
    //SELECT sum(deads) FROM ws_16.casualties;
    private static Integer calculateNoOfDead(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(deads) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer noOfDead = 0;
        while (resultSet.next()) {
            noOfDead = Math.round(resultSet.getFloat(1));
        }
        return noOfDead;
    }

    //No Of Homeless formula: from table casualties sum each row from column homeless
    //SELECT sum(homeless) FROM ws_16.casualties;
    private static Integer calculateNoOfHomeless(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(homeless) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer noOfHomeless = 0;
        while (resultSet.next()) {
            noOfHomeless = Math.round(resultSet.getFloat(1));
        }
        return noOfHomeless;
    }

    //No Of Injuried formula: from table casualties sum each row from column injuried
    //SELECT sum(injuried) FROM ws_16.casualties;
    private static Integer calculateNoOfInjured(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(injuried) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer noOfInjured = 0;
        while (resultSet.next()) {
            noOfInjured = Math.round(resultSet.getFloat(1));
        }
        return noOfInjured;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD';
    private static Integer calculateIndirectDamageCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer indirectDamageCost = 0;
        while (resultSet.next()) {
            indirectDamageCost = Math.round(resultSet.getFloat(1));
        }
        return indirectDamageCost;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='EVACUATION POST-EQ' 
    //OR cost='EMERGENCY MANAGEMENT' OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'
    //OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'
    private static Integer calculateDirectDamageCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='EVACUATION POST-EQ' OR cost='EMERGENCY MANAGEMENT' "
                        + "OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'"
                        + "OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer directDamageCost = 0;
        while (resultSet.next()) {
            directDamageCost = Math.round(resultSet.getFloat(1));
        }
        return directDamageCost;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='RECONSTRUCTION'
    private static Integer calculateDirectRestorationCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT value FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='RECONSTRUCTION'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Integer directRestorationCost = 0;
        while (resultSet.next()) {
            directRestorationCost = Math.round(resultSet.getFloat(1));
        }
        return directRestorationCost;
    }

    public static Indicators calculateIndicators(String worldStateName, Connection connection)
            throws Exception {
        Statement statement = null;
        ResultSet resultSet = null;
        Indicators indicators = null;
        try {
            statement = connection.createStatement();
            Integer lostBuildingsAvg = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage");
            Integer lostBuildingsMax = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage_varmax");
            Integer lostBuildingsMin = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage_varmin");

            Integer unsafeBuildingsAvg = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage");
            Integer unsafeBuildingsMax = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage_varmax");
            Integer unsafeBuildingsMin = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage_varmin");

            Integer noOfDeadAvg = calculateNoOfDead(worldStateName, resultSet, statement, "casualties");
            Integer noOfDeadMax = calculateNoOfDead(worldStateName, resultSet, statement, "casualties_varmax");
            Integer noOfDeadMin = calculateNoOfDead(worldStateName, resultSet, statement, "casualties_varmin");

            Integer noOfHomelessAvg = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties");
            Integer noOfHomelessMax = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties_varmax");
            Integer noOfHomelessMin = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties_varmin");

            Integer noOfInjuredAvg = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties");
            Integer noOfInjuredMax = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties_varmax");
            Integer noOfInjuredMin = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties_varmin");

            //Calling the procedure to calculate the economic costs
            StringBuilder stringBuilder = new StringBuilder("SELECT aquila.v2_ec_tot_eq_cost('");
            stringBuilder.append(worldStateName).append("')");
            resultSet = statement.executeQuery(stringBuilder.toString());

            Integer indirectDamageCostAvg = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot");
            Integer indirectDamageCostMax = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Integer indirectDamageCostMin = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            Integer directDamageCostAvg = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot");
            Integer directDamageCostMax = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Integer directDamageCostMin = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            Integer directRestorationCostAvg = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot");
            Integer directRestorationCostMax = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Integer directRestorationCostMin = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            //TODO: Calculate economic cost from DB procedures
            Integer preemtiveEvacuationCost = 0;
            Integer noOfEvacuated = 0;
            Integer totalRetrofittingCost = 0;
            Integer noOfRetrofittedBuildings = 0;

            indicators = PilotDHelper.getIndicators(
                    noOfDeadMin, noOfInjuredMin, noOfHomelessMin,
                    noOfDeadAvg, noOfInjuredAvg, noOfHomelessAvg,
                    noOfDeadMax, noOfInjuredMax, noOfHomelessMax,
                    directDamageCostMin, indirectDamageCostMin, directRestorationCostMin,
                    directDamageCostAvg, indirectDamageCostAvg, directRestorationCostAvg,
                    directDamageCostMax, indirectDamageCostMax, directRestorationCostMax,
                    lostBuildingsMin, unsafeBuildingsMin,
                    lostBuildingsAvg, unsafeBuildingsAvg,
                    lostBuildingsMax, unsafeBuildingsMax,
                    preemtiveEvacuationCost, noOfEvacuated,
                    totalRetrofittingCost, noOfRetrofittedBuildings);
        } catch (Exception e) {
            /**
             * This try catch statement is only usefull to close the resultset
             * connections, it will throw all the execptions
             */
            throw e;
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException sql) {
            }
        }

        return indicators;
    }

}
