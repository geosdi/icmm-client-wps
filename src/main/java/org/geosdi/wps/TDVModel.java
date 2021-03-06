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
import eu.crismaproject.icmm.icmmhelper.pilotD.Indicators;
import eu.crismaproject.icmm.icmmhelper.pilotD.PilotDHelper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import jersey.repackaged.com.google.common.collect.Lists;
import org.geosdi.wps.utility.IndicatorCalculator;
import org.geosdi.wps.utility.GeoServerUtils;
import org.geosdi.wps.utility.ICMMHelperFacade;
import org.geosdi.wps.utility.ShakeMapNameEnum;
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
     * curl -u admin:m_Loa5hJz8Vb -H 'Content-type: xml' -XPOST -d@'/home/andypower/tdv_wps_Sync.xml' http://wps.plinivs.it:8080/geoserver/wps
     */
    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private int PROCESS_PHASES = 2;

    private GeoServerUtils geoServerUtils;
    private ICMMHelperFacade icmmHelperFacade;

    public TDVModel(GeoServerUtils geoServerUtils, ICMMHelperFacade icmmHelperFacade) {
        this.geoServerUtils = geoServerUtils;
        this.icmmHelperFacade = icmmHelperFacade;
    }

    @DescribeResult(name = "result", description = "String that indicates the "
            + "result state of the execution")
    public String execute(
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

        this.PROCESS_PHASES += noOfEvents * 5;

        Format formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date startDate = new Date();

        //*WF* Generating transition object && write transition object to ICMM
        Transition transition = this.icmmHelperFacade.initProcessTransition(
                "Time Dependent Vulnerability Elaboration",
                "WPS TDV Elaboration, started on " + formatter.format(startDate),
                this.PROCESS_PHASES);

        //Init basic elements to publish layers
        WorkspaceInfo crismaWorkspace = this.geoServerUtils.getWorkspace();
        //create a namespace corresponding to the workspace if one does not 
        // already exist
        NamespaceInfo namespace = this.geoServerUtils.getNamespace(crismaWorkspace);

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = this.geoServerUtils.connectToDatabaseOrDie();
            statement = connection.createStatement();
            int targetICMMWorldStateID = wsId;
            int targetDBWorldStateID = -1;

            int i = 1;
            int operation = 1;
            SimpleFeatureIterator iterator = eqTDVParList.features();
            while (iterator.hasNext()) {
                //*WF* Fetch origin worldstate (WS) from ICMM
                logger.log(Level.INFO, "Getting ICMM world state with id: " + targetICMMWorldStateID);
                final Worldstate originWs = this.icmmHelperFacade.getClient().getWorldstate(targetICMMWorldStateID, 3, true);
                //*WF* Extract origin schema from WS
                String originICMMSchema = PilotDHelper.getSchema(originWs);
                logger.log(Level.INFO, "DB Origin Schema: " + originICMMSchema);
                logger.log(Level.INFO, "After world state fetching");

                //*WF* Update CCIM transition object: Preparing workspace for round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Preparing workspace",
                        transition, operation + 1, PROCESS_PHASES, Transition.Status.RUNNING);

                SimpleFeature eqTDVPar = iterator.next();
                final List<DataItem> resultItems = Lists.<DataItem>newArrayList();
                //Executing World State copy
                resultSet = statement.executeQuery("select aquila.ccr_ws_mk('" + originICMMSchema + "', 2)");
                logger.log(Level.FINEST, "Creating World State copy, result metadata column count: "
                        + resultSet.getMetaData().getColumnCount());
                while (resultSet.next()) {
                    targetDBWorldStateID = resultSet.getInt(1);
                }
                String worldStateSchemaOnDB = this.icmmHelperFacade.generateWorldStateName(targetDBWorldStateID);
                logger.log(Level.INFO, "Result for world state copy operation: " + targetDBWorldStateID);
//                Thread.sleep(30000);
                //*WF* Write target schema dataItem to ICMM
                DataItem schemaItem = this.icmmHelperFacade.writeTargetSchemaDataItem(worldStateSchemaOnDB);
                resultItems.add(schemaItem);

                //*WF* Update CCIM transition object: Running build damage model round round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Running build damage model round: " + i,
                        transition, operation + 2, PROCESS_PHASES, Transition.Status.RUNNING);

                // execute building damage
                DataStoreInfo crismaDatastore = this.geoServerUtils.getDataStore(crismaWorkspace, worldStateSchemaOnDB);

                /*
                 aquila.v2_building_damage(sch_name text, nstep integer, shakemap boolean, 
                 map_name text, lat real, lon real, depth real, magnitudo real)
                 */
                boolean useShakeMap = (Boolean) eqTDVPar.getAttribute("useShakemap");
                String shakeMapName = (String) eqTDVPar.getAttribute("ShakeMapName");
                Number latitude = (Number) eqTDVPar.getAttribute("Latitude");
                Number longitude = (Number) eqTDVPar.getAttribute("Longitude");
                Number magnitude = (Number) eqTDVPar.getAttribute("Magnitude");
                Number depth = (Number) eqTDVPar.getAttribute("Depth");
//                logger.log(Level.INFO, "Params to elaborate: " + useShakeMap + 
//                        shakeMapName + longitude + latitude + 
//                        magnitude + depth);

                String shakeMapEnumValue = null;
                if (shakeMapName != null) {
                    //This throws IllegalArgumentException iff the shakeMapName 
                    //does not correspont to any accepted enum
                    shakeMapEnumValue = ShakeMapNameEnum.valueOf(shakeMapName).getName();
                }
                StringBuilder stringBuilder = new StringBuilder("select aquila.v2_building_damage('");
                stringBuilder.
                        append(worldStateSchemaOnDB).
                        append("',").
                        append(i).
                        append(",").
                        append(useShakeMap).
                        append(",").
                        append(shakeMapEnumValue == null ? "''" : "'" + shakeMapEnumValue + "'").
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
                logger.log(Level.INFO, "Query Executed: " + stringBuilder.toString());
                while (resultSet.next()) {
                    //Catching column 1 value
                    logger.log(Level.INFO, resultSet.getString(1));
                }
                //*WF* Publishing intensity grid, building damage (min/max/avg) on WMS
                FeatureTypeInfo featureTypeInfo = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "intens_grid",
                        "intens_grid");

                //*WF* Write intensity grid, building damage (min/max/avg) dataitems to ICMM
                String intensGridName = featureTypeInfo.getName();
                DataItem dataItem = this.icmmHelperFacade.writeWMSDataItem(
                        intensGridName, "Intensity Grid", Categories.INTENSITY_GRID);
                resultItems.add(dataItem);

                featureTypeInfo = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace,
                        "building_damage_varmin", "building_damage_lost");
                //
                dataItem = this.icmmHelperFacade.writeWMSDataItem(
                        featureTypeInfo.getName(), "Lost Buildings (Min)", Categories.BUILDING_DAMAGE_MIN);
                resultItems.add(dataItem);

                featureTypeInfo = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace,
                        "building_damage", "building_damage_lost");
                //
                dataItem = this.icmmHelperFacade.writeWMSDataItem(
                        featureTypeInfo.getName(), "Lost Buildings (Avg)", Categories.BUILDING_DAMAGE_AVG);
                resultItems.add(dataItem);

                featureTypeInfo = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace,
                        "building_damage_varmax", "building_damage_lost");
                //
                dataItem = this.icmmHelperFacade.writeWMSDataItem(
                        featureTypeInfo.getName(), "Lost Buildings (Max)", Categories.BUILDING_DAMAGE_MAX);
                resultItems.add(dataItem);
//Example WMS link: http://192.168.1.30:8080/geoserver/wms?request=GetMap&service=WMS&version=1.1.1&layers=crisma:intens_grid&format=image%2Fpng&bbox=345220.145083,4670346.1361,391220.145083,4716846.1361&width=506&height=512&srs=EPSG:32633
                //*WF* Update CCIM transition object: Running building inventory update round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Running building inventory update for round: " + i,
                        transition, operation + 3, PROCESS_PHASES, Transition.Status.RUNNING);

                //*WF* Update the building inventory
                stringBuilder = new StringBuilder("select aquila.v2_ooi_update('");
                stringBuilder.append(worldStateSchemaOnDB).append("')");
                logger.log(Level.INFO, "Building Inventory Query Executed: " + stringBuilder.toString());
                resultSet = statement.executeQuery(stringBuilder.toString());

                //*WF* Publish building inventory on WMS
                FeatureTypeInfo ooiUpdateFeatures = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "building_inventory",
                        "buildings_inventory_eqvclasses");
                //*WF* Write building inventory dataitems to ICMM
                String buildingInventoryName = ooiUpdateFeatures.getName();
                DataItem buildingInventoryItem = this.icmmHelperFacade.writeWMSDataItem(
                        buildingInventoryName, "Building Inventory", Categories.BUILDING_INVENTORY);
                resultItems.add(buildingInventoryItem);

                //*WF* Update CCIM transition object: Running people impact for round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Running people impact for round: " + i,
                        transition, operation + 4, PROCESS_PHASES, Transition.Status.RUNNING);

                //*WF* Execute people impact procedure
                stringBuilder = new StringBuilder("select aquila.v2_casualties('");
                stringBuilder.
                        append(worldStateSchemaOnDB).
                        append("',").
                        append(i).
                        append(")");
                resultSet = statement.executeQuery(stringBuilder.toString());

                //*WF* Publish people impact (min/max/avg) on WMS
                //*WF* Write people impact (min/max/avg) dataitems to ICMM
                FeatureTypeInfo casualitiesFeatures = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace, "casualties",
                        "casualties_deads");

                String peopleImpactAVGName = casualitiesFeatures.getName();
                DataItem peopleImpactDataItem = this.icmmHelperFacade.writeWMSDataItem(
                        peopleImpactAVGName, "Deads (Avg)", Categories.PEOPLE_IMPACT_AVG);
                resultItems.add(peopleImpactDataItem);

                casualitiesFeatures = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace,
                        "casualties_varmin", "casualties_deads");
                String peopleImpactMinName = casualitiesFeatures.getName();;
                peopleImpactDataItem = this.icmmHelperFacade.writeWMSDataItem(
                        peopleImpactMinName, "Deads (Min)", Categories.PEOPLE_IMPACT_MIN);
                resultItems.add(peopleImpactDataItem);

                casualitiesFeatures = this.geoServerUtils.getOrPublishFeatureType(
                        crismaWorkspace, crismaDatastore, namespace,
                        "casualties_varmax", "casualties_deads");
                String peopleImpactMaxName = casualitiesFeatures.getName();
                peopleImpactDataItem = this.icmmHelperFacade.writeWMSDataItem(
                        peopleImpactMaxName, "Deads (Max)", Categories.PEOPLE_IMPACT_MAX);
                resultItems.add(peopleImpactDataItem);
                //
                FeatureTypeInfo peopleDistributionFeatures = this.geoServerUtils.
                        getOrPublishFeatureType(crismaWorkspace, crismaDatastore,
                                namespace, "comp_cell", "people_distrib");
                String peopleDistributionName = peopleDistributionFeatures.getName();
                peopleImpactDataItem = this.icmmHelperFacade.writeWMSDataItem(
                        peopleDistributionName, "People Distribution", Categories.PEOPLE_DISTRIBUTION);
                resultItems.add(peopleImpactDataItem);

                //*WF* Update CCIM transition object: Calculating indicators for round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Calculating indicators for round: " + i,
                        transition, operation + 5, PROCESS_PHASES, Transition.Status.RUNNING);

                //TODO: Waiting for procedures to complete the code that calculates the indicators
                Indicators indicators = IndicatorCalculator.calculateIndicators(
                        worldStateSchemaOnDB, connection, true);
                logger.log(Level.INFO, "Calculated indicators: {0}", indicators);
                //*WF* Write indicator dataitems to ICMM
                DataItem indicatorsDataItem = this.icmmHelperFacade.writeIndicatorsDataItem(indicators);

                final Worldstate targetWs = PilotDHelper.getWorldstate(originWs, resultItems,
                        indicatorsDataItem, transition, "WS " + targetDBWorldStateID, 
                        "This is the world state representation");
                this.icmmHelperFacade.getClient().insertSelfRefAndId(targetWs);
                logger.finest("WORLD STATE: " + targetWs);
                //*WF* Write new World State to ICMM
                this.icmmHelperFacade.persistWorldState(targetWs, originWs);
                targetICMMWorldStateID = targetWs.getId();

                i++;
                operation += 5;
            }
            //*WF* Update CCIM transition object: Status finished
            //&& Write transition object to ICMM
            transition.setDescription("WPS TDV Elaboration, started on "
                    + formatter.format(startDate)
                    + ", ended on " + formatter.format(new Date()));
            this.icmmHelperFacade.updateTransition("Process Executed",
                    transition, PROCESS_PHASES,
                    PROCESS_PHASES, Transition.Status.FINISHED);
            return "Process Executed correctly";
        } catch (Exception e) {
            logger.log(Level.SEVERE, "TDV Exception: {0}", e);
            logger.log(Level.SEVERE, "StackTrace: {0}", Arrays.toString(e.getStackTrace()));
            transition.setDescription("WPS TDV Elaboration, started on "
                    + formatter.format(startDate)
                    + ", failed on " + formatter.format(new Date()));
            this.icmmHelperFacade.updateTransition("Exception: " + e, transition, PROCESS_PHASES,
                    PROCESS_PHASES, Transition.Status.ERROR);
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
        return "Errors occured executing the process see logs for dettails";
    }
}
