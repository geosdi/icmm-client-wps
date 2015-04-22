/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps.utility;

import eu.crismaproject.icmm.icmmhelper.ICMMClient;
import eu.crismaproject.icmm.icmmhelper.ICMMHelper;
import eu.crismaproject.icmm.icmmhelper.entity.DataItem;
import eu.crismaproject.icmm.icmmhelper.entity.Transition;
import eu.crismaproject.icmm.icmmhelper.pilotD.Categories;
import eu.crismaproject.icmm.icmmhelper.pilotD.Indicators;
import eu.crismaproject.icmm.icmmhelper.pilotD.PilotDHelper;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class ICMMHelperFacade {

    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private final ICMMClient client;
    private final boolean debug;

    public ICMMHelperFacade(String icmm_url, boolean debug) {
        this.client = new ICMMClient(icmm_url);
        this.debug = debug;
    }

    private void debug() {
        if (this.debug) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, "Error sleeping thread: " + ex);
            }
        }
    }

    public Transition initProcessTransition(String transitionName,
            String transitionDescription, int processStepsNumber) {
        Transition transition = ICMMHelper.createTransition(transitionName,
                transitionDescription);
        //Remember to initialize transaction Self Ref ID
        client.insertSelfRefAndId(transition);
        client.putTransition(transition);
        //Remember to push the result on client side
        ICMMHelper.updateTransition(transition, Transition.Status.RUNNING, 1,
                processStepsNumber, "Starting process " + transitionName);
        client.putTransition(transition);
        this.debug();
        return transition;
    }

    public DataItem writeTargetSchemaDataItem(String worldStateName) {
        final DataItem schemaItem = PilotDHelper.getSchemaItem(worldStateName);
        this.client.insertSelfRefAndId(schemaItem);
        this.client.putEntity(schemaItem);
        this.debug();
        return schemaItem;
    }

    public DataItem writeIndicatorsDataItem(Indicators indicators) {
        DataItem indicatorsDataItem = PilotDHelper.getIndicatorDataItem(indicators);
        this.client.insertSelfRefAndId(indicatorsDataItem);
        this.client.putEntity(indicatorsDataItem);
        this.debug();
        return indicatorsDataItem;
    }

    public DataItem writeWMSDataItem(String layerName, String displayName,
            Categories category) {
        final DataItem wmsDataItem = PilotDHelper.getWmsDataItem(
                layerName, displayName, category);
        this.client.insertSelfRefAndId(wmsDataItem);
        this.client.putEntity(wmsDataItem);
        this.debug();
        return wmsDataItem;
    }

    public void updateTransition(String message, Transition transition, int runningPhase,
            int processStepsNumber, Transition.Status status) {
        ICMMHelper.updateTransition(transition, status, runningPhase,
                processStepsNumber, message);
        client.putTransition(transition);
        this.debug();
    }

    public String generateWorldStateName(int wsId) {
        return "ws_" + wsId;
    }

    public ICMMClient getClient() {
        return client;
    }

}
