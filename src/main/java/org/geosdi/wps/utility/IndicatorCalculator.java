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

    private static long round(double value) {
        double temp = Math.pow(10, 0);
        return Math.round(value * temp) / (long) temp;
    }

    //Lost builings formula: from table building damage sum the total from columns nd4+nd5
    //SELECT sum(nd4 + nd5) FROM ws_16.building_damage;
    private static Long calculateLostBuildings(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(nd4 + nd5) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long lostBuildings = 0L;
        while (resultSet.next()) {
            lostBuildings = round(resultSet.getDouble(1));
        }
        return lostBuildings;
    }

    //Unsafe Buildings formula: from table building damage sum the total from columns ((0.5 * nd3) + nd4 + nd5)
    //SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ws_1.building_damage;
    private static Long calculateUnsafeBuildings(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long unsafeBuildings = 0L;
        while (resultSet.next()) {
            unsafeBuildings = round(resultSet.getDouble(1));
        }
        return unsafeBuildings;
    }

    //No Of Dead formula: from table casualties sum each row from column deads
    //SELECT sum(deads) FROM ws_16.casualties;
    private static Long calculateNoOfDead(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(deads) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long noOfDead = 0L;
        while (resultSet.next()) {
            noOfDead = round(resultSet.getDouble(1));
        }
        return noOfDead;
    }

    //No Of Homeless formula: from table casualties sum each row from column homeless
    //SELECT sum(homeless) FROM ws_16.casualties;
    private static Long calculateNoOfHomeless(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(homeless) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long noOfHomeless = 0L;
        while (resultSet.next()) {
            noOfHomeless = round(resultSet.getDouble(1));
        }
        return noOfHomeless;
    }

    //No Of Injuried formula: from table casualties sum each row from column injuried
    //SELECT sum(injuried) FROM ws_16.casualties;
    private static Long calculateNoOfInjured(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(injuried) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName);
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long noOfInjured = 0L;
        while (resultSet.next()) {
            noOfInjured = round(resultSet.getDouble(1));
        }
        return noOfInjured;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD';
    private static Long calculateIndirectDamageCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long indirectDamageCost = 0L;
        while (resultSet.next()) {
            indirectDamageCost = round(resultSet.getDouble(1));
        }
        return indirectDamageCost;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='EVACUATION POST-EQ' 
    //OR cost='EMERGENCY MANAGEMENT' OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'
    //OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'
    private static Long calculateDirectDamageCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='EVACUATION POST-EQ' OR cost='EMERGENCY MANAGEMENT' "
                        + "OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'"
                        + "OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long directDamageCost = 0L;
        while (resultSet.next()) {
            directDamageCost = round(resultSet.getDouble(1));
        }
        return directDamageCost;
    }

    //select sum(value) FROM ws_1.ec_tot where cost='RECONSTRUCTION'
    private static Long calculateDirectRestorationCost(String worldStateName, ResultSet resultSet,
            Statement statement, String tableName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT value FROM ");
        stringBuilder.append(worldStateName).append(".").append(tableName).
                append(" where cost='RECONSTRUCTION'");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long directRestorationCost = 0L;
        while (resultSet.next()) {
            directRestorationCost = round(resultSet.getDouble(1));
        }
        return directRestorationCost;
    }

    private static Long calculateTotalRetrofittingCost(String worldStateName,
            ResultSet resultSet, Statement statement) throws Exception {
        StringBuilder stringBuilder = new StringBuilder(
                "SELECT round(sum(coalesce(cl_tot,0))) build_mitig_totcost from "
                + "(select b+c+d+den1+den2 cl_tot from ");
        stringBuilder.append(worldStateName).append(".dc_buil_miti_output1) q");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long totalRetrofittingCost = 0L;
        while (resultSet.next()) {
            totalRetrofittingCost = round(resultSet.getDouble(1));
        }
        return totalRetrofittingCost;
    }

    private static Long calculateNoOfRetrofittedBuildings(String worldStateName,
            ResultSet resultSet, Statement statement) throws Exception {
        StringBuilder stringBuilder = new StringBuilder(
                "SELECT round(sum(coalesce(tot_buil_miti,0))) from ");
        stringBuilder.append(worldStateName).append(".dc_buil_miti_output2");
        resultSet = statement.executeQuery(stringBuilder.toString());
        Long noOfRetrofittedBuildings = 0L;
        while (resultSet.next()) {
            noOfRetrofittedBuildings = round(resultSet.getDouble(1));
        }
        return noOfRetrofittedBuildings;
    }

    private static ResultSet calculateEvacuation(String worldStateName,
            ResultSet resultSet, Statement statement) throws Exception {
        StringBuilder stringBuilder = new StringBuilder(
                "select round(coalesce(abs(miti_cost),0.0)) preem_evac_cost, coalesce(n_evac_people,0) num_evac from ");
        stringBuilder.append(worldStateName).append(".cost_bene_miti_evac_out");
        resultSet = statement.executeQuery(stringBuilder.toString());
        return resultSet;
    }

    public static Indicators calculateIndicators(String worldStateName, Connection connection,
            boolean calculateEconomicCosts)
            throws Exception {
        Statement statement = null;
        ResultSet resultSet = null;
        Indicators indicators = null;
        try {
            statement = connection.createStatement();
            Long lostBuildingsAvg = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage");
            Long lostBuildingsMax = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage_varmax");
            Long lostBuildingsMin = calculateLostBuildings(worldStateName, resultSet, statement, "building_damage_varmin");

            Long unsafeBuildingsAvg = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage");
            Long unsafeBuildingsMax = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage_varmax");
            Long unsafeBuildingsMin = calculateUnsafeBuildings(worldStateName, resultSet, statement, "building_damage_varmin");

            Long noOfDeadAvg = calculateNoOfDead(worldStateName, resultSet, statement, "casualties");
            Long noOfDeadMax = calculateNoOfDead(worldStateName, resultSet, statement, "casualties_varmax");
            Long noOfDeadMin = calculateNoOfDead(worldStateName, resultSet, statement, "casualties_varmin");

            Long noOfHomelessAvg = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties");
            Long noOfHomelessMax = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties_varmax");
            Long noOfHomelessMin = calculateNoOfHomeless(worldStateName, resultSet, statement, "casualties_varmin");

            Long noOfInjuredAvg = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties");
            Long noOfInjuredMax = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties_varmax");
            Long noOfInjuredMin = calculateNoOfInjured(worldStateName, resultSet, statement, "casualties_varmin");

            if (calculateEconomicCosts) {
                //Calling the procedure to calculate the economic costs
                StringBuilder stringBuilder = new StringBuilder("SELECT aquila.v2_ec_tot_eq_cost('");
                stringBuilder.append(worldStateName).append("')");
                resultSet = statement.executeQuery(stringBuilder.toString());
            }

            Long indirectDamageCostAvg = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot");
            Long indirectDamageCostMax = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Long indirectDamageCostMin = calculateIndirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            Long directDamageCostAvg = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot");
            Long directDamageCostMax = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Long directDamageCostMin = calculateDirectDamageCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            Long directRestorationCostAvg = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot");
            Long directRestorationCostMax = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot_varmax");
            Long directRestorationCostMin = calculateDirectRestorationCost(worldStateName, resultSet, statement, "ec_tot_varmin");

            Long totalRetrofittingCost = calculateTotalRetrofittingCost(worldStateName, resultSet, statement);

            Long noOfRetrofittedBuildings = calculateNoOfRetrofittedBuildings(worldStateName, resultSet, statement);

            Long preemtiveEvacuationCost = 0L;
            Long noOfEvacuated = 0L;
            ResultSet resul = calculateEvacuation(worldStateName, resultSet, statement);
            while (resul.next()) {
                preemtiveEvacuationCost = round(resul.getDouble(1));
                noOfEvacuated = round(resul.getDouble(2));
            }

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
