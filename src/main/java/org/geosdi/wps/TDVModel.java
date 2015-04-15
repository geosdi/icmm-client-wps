/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import eu.crismaproject.icmm.icmmhelper.entity.DataItem;
import eu.crismaproject.icmm.icmmhelper.entity.Transition;
import eu.crismaproject.icmm.icmmhelper.entity.Worldstate;
import eu.crismaproject.icmm.icmmhelper.pilotD.Categories;
import eu.crismaproject.icmm.icmmhelper.pilotD.PilotDHelper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import jersey.repackaged.com.google.common.collect.Lists;
import org.geosdi.wps.utility.Utils;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

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

    @DescribeResult(name = "intens grid", description = "WFS link for intensity distribution map")
    public String execute(
            @DescribeParameter(name = "applyTDV", description
                    = "Indicates whether the TDV model shall be used to calculate "
                    + "a sequence of events with updated inventory") boolean applyTDV,
            @DescribeParameter(name = "noOfEvents", description = "The number of events "
                    + "that shall be simulated by the TDV model, not evaluated "
                    + "if apply TDV is false. Defaults to '1' (one). "
                    + "If noOfEvents > 1 then a set of () parameters must be "
                    + "included in the parameters list.") int noOfEvents,
            @DescribeParameter(name = "eqTDVParList", description = "A SimpleFeatureCollection"
                    + "having number of features equals to the param noOfEvents "
                    + "value. Each feature contains TDV parameters that specify the "
                    + "characteristics of the earthquakes elements list.") SimpleFeatureCollection eqTDVParList,
            @DescribeParameter(name = "wsID", description = "WS ID") Integer wsId)
            throws Exception {
        
        this.PROCESS_PHASES += noOfEvents * 3;

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
            
            int i = 0;
            SimpleFeatureIterator iterator = eqTDVParList.features();
            while (iterator.hasNext()) {
                SimpleFeature eqTDVPar = iterator.next();
                final List<DataItem> resultItems = Lists.<DataItem>newArrayList();
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
                //TODO: Try this code
//                final DataItem schemaItem = PilotDHelper.getSchemaItem(worldStateName);
//                this.utils.getClient().insertSelfRefAndId(schemaItem);
//                this.utils.getClient().putEntity(schemaItem);
//                resultItems.add(schemaItem);

                // execute building damage
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
                boolean useShakeMap = (Boolean) eqTDVPar.getAttribute("useShakemap");
                String shakeMapName = (String) eqTDVPar.getAttribute("ShakeMapName");
                Double latitude = (Double) eqTDVPar.getAttribute("Latitude");
                Double longitude = (Double) eqTDVPar.getAttribute("Longitude");
                Double magnitude = (Double) eqTDVPar.getAttribute("Magnitude");
                Double depth = (Double) eqTDVPar.getAttribute("Depth");
                logger.info("************** " + useShakeMap + shakeMapName + longitude + latitude + magnitude + depth);
                
                StringBuilder stringBuilder = new StringBuilder("select aquila.v2_building_damage('");
                stringBuilder.
                        append(worldStateName).
                        append("',").
                        append(i).//TODO: check if the nstep integer value referes to the number of iteration
                        append(",").
                        append(useShakeMap).
                        append(",").
                        append(shakeMapName == null ? "''" : "'" + shakeMapName + "'").
                        append(",").
                        append(latitude).
                        append(",").
                        append(longitude).
                        append(",").
                        append(depth).
                        append(",").
                        append(magnitude).
                        append(")");
                resultSet = statement.executeQuery(stringBuilder.toString());
                logger.info("Query Executed: " + stringBuilder.toString());
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
                
                final DataItem itensityGridItem = PilotDHelper.getWmsDataItem(
                        intensGridName, "Intensity Grid", Categories.INTENSITY_GRID);
                this.utils.getClient().insertSelfRefAndId(itensityGridItem);
                this.utils.getClient().putEntity(itensityGridItem);
                resultItems.add(itensityGridItem);
                
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
                i++;
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
