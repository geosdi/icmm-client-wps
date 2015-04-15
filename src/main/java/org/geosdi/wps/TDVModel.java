/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import eu.crismaproject.icmm.icmmhelper.entity.Transition;
import eu.crismaproject.icmm.icmmhelper.entity.Worldstate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.geosdi.wps.utility.Utils;
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
@DescribeProcess(title = "TDV Model", description = "CRISMA WPS for Time Dependent Vulnerability elaboration")
public class TDVModel implements GeoServerProcess {

    /*
     * Example link to execute the WPS:
     * http://192.168.1.30:8080/geoserver/wps?service=WPS&version=1.0.0&request=Execute&identifier=gs:HazardModel&datainputs=isShakemap=true;shakeMapName=name;latitude=5;magnitude=2;longitude=5;depth=10;
     * Example cURL request:
     * curl -u admin:m_Loa5hJz8Vb -H 'Content-type: xml' -XPOST -d@'/home/andypower/tdv_wps.xml' http://wps.plinivs.it:8080/geoserver/wps
     */
    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private int PROCESS_PHASES = 3;

    private Utils utils;

    public TDVModel(Utils utils) {
        this.utils = utils;
    }

    //Create a complex type for base parameters and receive here 3 params: 
    //wsID, ComplexType and num of iterations
    @DescribeResult(name = "intens grid", description = "WFS link for intensity distribution map")
    public String execute(
            @DescribeParameter(name = "isShakemap", description = "Shakemap presence") boolean isShakeMap,
            @DescribeParameter(name = "shakeMapName", description = "Shakemap table name",
                    collectionType = String.class, min = 0) List<String> shakeMapNameList,
            @DescribeParameter(name = "latitude", description = "Epi center latitude",
                    collectionType = Float.class) List<Float> latList,
            @DescribeParameter(name = "longitude", description = "Epi center longitude",
                    collectionType = Float.class) List<Float> lonList,
            @DescribeParameter(name = "depth", description = "Epi center longitude",
                    collectionType = Float.class) List<Float> depthList,
            @DescribeParameter(name = "magnitude", description = "Earthquake magnitude",
                    collectionType = Float.class) List<Float> magList,
            @DescribeParameter(name = "wsID", description = "WS ID") Integer wsId)
            throws Exception {

        this.PROCESS_PHASES += shakeMapNameList.size();

//        if (shakeMapName != null && shakeMapName.size() > 0) {
//            final String[] names = (String[]) shakeMapName.toArray(new String[shakeMapName.size()]);
//            logger.info("names: " + names.length);
//            logger.info("shakeMapName 1: " + names);
//            logger.info("shakeMapName 2: " + names[0]);
//            logger.info("shakeMapName 3: " + names[1]);
//        }
//        logger.info("shakeMapName: " + shakeMapNameList.size());
//        logger.info("shakeMapName 1: " + shakeMapNameList);
//        logger.info("shakeMapName 2: " + shakeMapNameList.get(0));
//        logger.info("shakeMapName 3: " + shakeMapNameList.get(1));
        Transition transition = this.utils.initProcessTransition(
                "Time Dependent Vulnerability Elaboration",
                "WPS TDV Elaboration", this.PROCESS_PHASES);

        //START ICMM 
        //Manually creating the World State
//        Worldstate worldstate = new Worldstate();
//        worldstate.setId(id);
//        worldstate.set$self("//1");
//        this.utils.getClient().putWorldstate(worldstate); 
        //I'm asking for the existent WS having id 1 
        //waiting for the world state generation
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

        //Init basic elements to publish layers
        WorkspaceInfo crismaWorkspace = this.utils.getWorkspace();
        //create a namespace corresponding to the workspace if one does not 
        // already exist
        NamespaceInfo namespace = this.utils.getNamespace(crismaWorkspace);

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = this.utils.connectToDatabaseOrDie();
            statement = connection.createStatement();
            int targetWorldSateID = wsId;

            for (int i = 0; i < shakeMapNameList.size(); i++) {
                String worldStateName = this.utils.generateWorldStateName(targetWorldSateID);
                //Executing World State copy
                resultSet = statement.executeQuery("select aquila.ccr_ws_mk('" + worldStateName + "', 2)");
                while (resultSet.next()) {
                    logger.info("Creating World State copy, result metadata column count: "
                            + resultSet.getMetaData().getColumnCount());
                    targetWorldSateID = resultSet.getInt(1);
                    worldStateName = this.utils.generateWorldStateName(targetWorldSateID);
                    logger.info("Result for world state copy operation: " + targetWorldSateID);
                }
                DataStoreInfo crismaDatastore = this.utils.getDataStore(crismaWorkspace, worldStateName);

                /*
                 aquila.v2_building_damage(sch_name text, 
                 nstep integer, 
                 shakemap boolean, 
                 map_name text, 
                 lat real, 
                 lon real, 
                 depth real, 
                 magnitudo real)
                 */
                StringBuilder stringBuilder = new StringBuilder("select aquila.v2_building_damage('");
                stringBuilder.
                        append(worldStateName).
                        append("',").
                        append(i).//TODO: check if the nstep integer value referes to the number of iteration
                        append(",").
                        append(isShakeMap).
                        append(",").
                        append(shakeMapNameList.get(i) == null ? "''" : "'"
                                        + shakeMapNameList.get(i) + "'").
                        append(",").
                        append(latList.get(i)).
                        append(",").
                        append(lonList.get(i)).
                        append(",").
                        append(depthList.get(i)).
                        append(",").
                        append(magList.get(i)).
                        append(")");
                resultSet = statement.executeQuery(stringBuilder.toString());
                logger.info("Query Executed");
                while (resultSet.next()) {
                    //Catching column 1 value
                    logger.info("Metadata column count: " + resultSet.getMetaData().getColumnCount());
                    logger.info(resultSet.getString(1));
                }
                //Publishing results for each iteration
                FeatureTypeInfo featureTypeInfo = this.utils.getFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "intens_grid");
                //TODO: Use the name to push result on ICMM repo
                String intensGridName = featureTypeInfo.getName();
                featureTypeInfo = this.utils.getFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "builing_damage_varmin");
                featureTypeInfo = this.utils.getFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "builing_damage");
                featureTypeInfo = this.utils.getFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "builing_damage_varmax");
//Example WMS link: http://192.168.1.30:8080/geoserver/wms?request=GetMap&service=WMS&version=1.1.1&layers=crisma:intens_grid&format=image%2Fpng&bbox=345220.145083,4670346.1361,391220.145083,4716846.1361&width=506&height=512&srs=EPSG:32633
                this.utils.updateTransition("Fetching results for iteration: " + i + 1,
                        transition, 2 + i, PROCESS_PHASES, Transition.Status.RUNNING);
//        client.getWorldstate(1).
            }
        } catch (Exception e) {
            logger.warning("TDV Exception: " + e);
            logger.warning("StackTrace: " + Arrays.toString(e.getStackTrace()));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

//        LayerInfo l = catalog.getFactory().createLayer();
//        // l.setName("foo");
//        l.setResource(featureTypeInfo);
//
//        StyleInfo s = catalog.getStyleByName("foostyle");
//        l.setDefaultStyle(s);
//        catalog.add(l);
        this.utils.updateTransition("Process Executed", transition, PROCESS_PHASES,
                PROCESS_PHASES, Transition.Status.FINISHED);
        return "Process Executed correctly";
    }
}
