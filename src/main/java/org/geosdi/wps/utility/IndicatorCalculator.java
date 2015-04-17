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

    public static Indicators calculateIndicators(String worldStateName, Connection connection)
            throws Exception {
        Statement statement = null;
        ResultSet resultSet = null;
        Indicators indicators = null;
        try {
            //Lost builings formula: from table building damage sum the total from columns nd4+nd5
            //SELECT sum(nd4 + nd5) FROM ws_16.building_damage;
            statement = connection.createStatement();
            StringBuilder stringBuilder = new StringBuilder("SELECT sum(nd4 + nd5) FROM ");
            stringBuilder.append(worldStateName).append(".building_damage");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long lostBuildings = 0L;
            while (resultSet.next()) {
                lostBuildings = resultSet.getLong(1);
            }

            //Unsafe Buildings formula: from table building damage sum the total from columns ((0.5 * nd3) + nd4 + nd5)
            //SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ws_1.building_damage;
            stringBuilder = new StringBuilder("SELECT sum((0.5 * nd3) + nd4 + nd5) FROM ");
            stringBuilder.append(worldStateName).append(".building_damage");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long unsafeBuildings = 0L;
            while (resultSet.next()) {
                unsafeBuildings = resultSet.getLong(1);
            }

            //No Of Dead formula: from table casualties sum each row from column deads
            //SELECT sum(deads) FROM ws_16.casualties;
            stringBuilder = new StringBuilder("SELECT sum(deads) FROM ");
            stringBuilder.append(worldStateName).append(".casualties");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long noOfDead = 0L;
            while (resultSet.next()) {
                noOfDead = resultSet.getLong(1);
            }

            //No Of Homeless formula: from table casualties sum each row from column homeless
            //SELECT sum(homeless) FROM ws_16.casualties;
            stringBuilder = new StringBuilder("SELECT sum(homeless) FROM ");
            stringBuilder.append(worldStateName).append(".casualties");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long noOfHomeless = 0L;
            while (resultSet.next()) {
                noOfHomeless = resultSet.getLong(1);
            }

            //No Of Injuried formula: from table casualties sum each row from column injuried
            //SELECT sum(injuried) FROM ws_16.casualties;
            stringBuilder = new StringBuilder("SELECT sum(injuried) FROM ");
            stringBuilder.append(worldStateName).append(".casualties");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long noOfInjured = 0L;
            while (resultSet.next()) {
                noOfInjured = resultSet.getLong(1);
            }

            //Calling the procedure to calculate the economic costs
            stringBuilder = new StringBuilder("SELECT aquila.v2_ec_tot_eq_cost(");
            stringBuilder.append(worldStateName).append(")");
            resultSet = statement.executeQuery(stringBuilder.toString());

            //select sum(value) FROM ws_1.ec_tot where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD';
            stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
            stringBuilder.append(worldStateName).append(".ec_tot where cost='VA PSYCO EFFECT' OR cost='VA EVACUATION' OR cost='DEAD'");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long indirectDamageCost = 0L;
            while (resultSet.next()) {
                indirectDamageCost = resultSet.getLong(1);
            }

            //select sum(value) FROM ws_1.ec_tot where cost='EVACUATION POST-EQ' 
            //OR cost='EMERGENCY MANAGEMENT' OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'
            //OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'
            stringBuilder = new StringBuilder("SELECT sum(value) FROM ");
            stringBuilder.append(worldStateName).append(".ec_tot where "
                    + "cost='EVACUATION POST-EQ' OR cost='EMERGENCY MANAGEMENT' "
                    + "OR cost='RUMBLE CLEAN UP' OR cost='RECONSTRUCTION'"
                    + "OR cost='REHABILITATION' OR cost='SANITARY' OR cost='BACK HOME'");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long directDamageCost = 0L;
            while (resultSet.next()) {
                directDamageCost = resultSet.getLong(1);
            }

            stringBuilder = new StringBuilder("SELECT value FROM ");
            stringBuilder.append(worldStateName).append(".ec_tot where cost='RECONSTRUCTION'");
            resultSet = statement.executeQuery(stringBuilder.toString());
            Long directRestorationCost = 0L;
            while (resultSet.next()) {
                directRestorationCost = resultSet.getLong(1);
            }

            //TODO: Calculate economic cost from DB procedures
            Long preemtiveEvacuationCost = 0L;
            Long noOfEvacuated = 0L;
            Long totalRetrofittingCost = 0L;
            Long noOfRetrofittedBuildings = 0L;

            indicators = PilotDHelper.getIndicators(noOfDead,
                    noOfInjured, noOfHomeless, directDamageCost,
                    indirectDamageCost, directRestorationCost, lostBuildings,
                    unsafeBuildings, preemtiveEvacuationCost, noOfEvacuated,
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
