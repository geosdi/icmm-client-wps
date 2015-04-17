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
            //TODO: Calculate economic cost from DB procedures
            Long directDamageCost = 0L;
            Long indirectDamageCost = 0L;
            Long directRestorationCost = 0L;
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
