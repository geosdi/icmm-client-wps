/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import org.geosdi.wps.utility.Utils;
import eu.crismaproject.icmm.icmmhelper.entity.Transition;
import eu.crismaproject.icmm.icmmhelper.entity.Worldstate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
@DescribeProcess(title = "Hazard Model", description = "CRISMA WPS for Hazard Model elaboration")
public class HazardModel implements GeoServerProcess {

    /*
     * Example link to execute the WPS:
     * http://192.168.1.30:8080/geoserver/wps?service=WPS&version=1.0.0&request=Execute&identifier=gs:HazardModel&datainputs=isShakemap=true;shakeMapName=name;latitude=5;magnitude=2;longitude=5;depth=10;
     * Swaggle http://crisma.cismet.de/pilotD/icms/
     */
    private Logger logger = Logger.getLogger("org.geosdi.wps");
    private final int PROCESS_PHASES = 3;

    private Utils utils;

    public HazardModel(Utils utils) {
        this.utils = utils;
    }

    @DescribeResult(name = "intens grid", description = "WFS link for intensity distribution map")
    public String execute(
            @DescribeParameter(name = "isShakemap", description = "Shakemap presence") boolean isShakeMap,
            @DescribeParameter(name = "shakeMapName", description = "Shakemap table name", min = 0) String shakeMapName,
            @DescribeParameter(name = "latitude", description = "Epi center latitude") Float lat,
            @DescribeParameter(name = "longitude", description = "Epi center longitude") Float lon,
            @DescribeParameter(name = "depth", description = "Epi center longitude") Float depth,
            @DescribeParameter(name = "magnitude", description = "Earthquake magnitude") Float mag,
            @DescribeParameter(name = "wsID", description = "WS ID") Integer wsId)
            throws Exception {

        logger.info("Start WPS Process");

        Transition transition = this.utils.initProcessTransition(
                "Hazard Model Elaboration",
                "WPS Hazard Model Elaboration", PROCESS_PHASES);

        //START ICMM 
        //Creo il ws a manella 
//        Worldstate worldstate = new Worldstate();
//        worldstate.setId(id);
//        worldstate.set$self("//1");
//        this.utils.getClient().putWorldstate(worldstate); 
        //Richiedo il ws esistente  con id 1 in attesa di far funzionare 
        //la generazione dei world states
        Worldstate worldstate = this.utils.getClient().getWorldstate(1, PROCESS_PHASES, true);

        logger.info("After world state initialization");

//        String originSchema = PilotDHelper.getSchema(worldstate);
//        logger.info("Oirigin Schema: " + originSchema);
//        String targetSchema = null;//User originSchema to do some operations
//        DataItem targetDataItem = PilotDHelper.getSchemaItem(targetSchema);
//        this.utils.getClient().insertSelfRefAndId(targetDataItem);
//        this.utils.getClient().putEntity(targetDataItem); //foreach result
//        List<DataItem> resultDataItems = Lists.<DataItem>newArrayList();
//        resultDataItems.add(targetDataItem);
        //Waiting for API method to save the WS at the END of the operation
        //END ICMM
        logger.info("After ICMM Helper code");
        Connection conn = this.utils.connectToDatabaseOrDie();
        logger.info("Connecting to the DB");
        Statement st = conn.createStatement();
        logger.info("Create statement");
//        PreparedStatement prepareStatement = conn.prepareStatement("select aquila.hazard_elaboration(?,?,?,?,?,?)");
//        prepareStatement.setBoolean(1, isShakeMap);
//        prepareStatement.setString(2, shakeMapName == null ? "" : shakeMapName);
//        prepareStatement.setFloat(3, lat);
//        prepareStatement.setFloat(4, lon);
//        prepareStatement.setFloat(5, depth);
//        prepareStatement.setFloat(6, mag);
//
//        ResultSet rs = prepareStatement.executeQuery();
//        ResultSet rs = st.executeQuery("select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3)");
        //                append("aquila.ccr_ws_mk('ws_0', 2)").
        String worldStateName = this.utils.generateWorldStateName(wsId);
        //TODO: Copiare il world state prima di usarlo

        StringBuilder stringBuilder = new StringBuilder("select aquila.v2_hazard_elaboration('");
        stringBuilder.
                append(worldStateName).
                append("',").
                append(isShakeMap).
                append(",").
                append(shakeMapName == null ? "''" : "'" + shakeMapName + "'").
                append(",").
                append(lat).
                append(",").
                append(lon).
                append(",").
                append(depth).
                append(",").
                append(mag).
                append(")");
        ResultSet rs = st.executeQuery(stringBuilder.toString());
        logger.info("Query Executed");
        while (rs.next()) {
            logger.info("Column 1 returned ");
            logger.info("Metadata column count: " + rs.getMetaData().getColumnCount());
            logger.info(rs.getString(1));
        }
        this.utils.updateTransition("Fetching results", transition,
                2, PROCESS_PHASES, Transition.Status.RUNNING);
//        client.getWorldstate(1).
        rs.close();
        st.close();

        WorkspaceInfo crismaWorkspace = this.utils.getWorkspace();
        //create a namespace corresponding to the workspace if one does not 
        // already exist
        NamespaceInfo namespace = this.utils.getNamespace(crismaWorkspace);
        DataStoreInfo crismaDatastore = this.utils.getDataStore(crismaWorkspace, worldStateName);

        FeatureTypeInfo featureTypeInfo = this.utils.getOrPublishFeatureType(
                crismaWorkspace, crismaDatastore, namespace, worldStateName);
//        LayerInfo l = catalog.getFactory().createLayer();
//        // l.setName("foo");
//        l.setResource(featureTypeInfo);
//
//        StyleInfo s = catalog.getStyleByName("foostyle");
//        l.setDefaultStyle(s);
//        catalog.add(l);

        this.utils.updateTransition("Process Executed", transition, PROCESS_PHASES,
                PROCESS_PHASES, Transition.Status.FINISHED);

        //Example WMS link: http://192.168.1.30:8080/geoserver/wms?request=GetMap&service=WMS&version=1.1.1&layers=crisma:intens_grid&format=image%2Fpng&bbox=345220.145083,4670346.1361,391220.145083,4716846.1361&width=506&height=512&srs=EPSG:32633
        return featureTypeInfo.getName();
    }

}
