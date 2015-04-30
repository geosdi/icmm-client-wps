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
@DescribeProcess(title = "World State Publication",
        description = "CRISMA WPS for publishing world states")
public class WorldStatePublication implements GeoServerProcess {

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

    public WorldStatePublication(GeoServerUtils geoServerUtils, ICMMHelperFacade icmmHelperFacade) {
        this.geoServerUtils = geoServerUtils;
        this.icmmHelperFacade = icmmHelperFacade;
    }

    @DescribeResult(name = "result", description = "String that indicates the "
            + "result state of the execution")
    public String execute(
            @DescribeParameter(name = "worldStateList", description = "ID of the world states to publish", collectionType = Integer.class) List<Integer> worldStateList)
            throws Exception {

        if (worldStateList == null || worldStateList.isEmpty()) {
            throw new IllegalArgumentException("The passed world state id list is empty");
        }
        this.PROCESS_PHASES += worldStateList.size() * 5;

        Format formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date startDate = new Date();

        //*WF* Generating transition object && write transition object to ICMM
        Transition transition = this.icmmHelperFacade.initProcessTransition(
                "World State Publication",
                "WPS World State Publication, started on " + formatter.format(startDate),
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

            //*WF* Fetch origin worldstate (WS) from ICMM
            logger.log(Level.INFO, "Getting ICMM world state with id: " + 0);
            final Worldstate originWs = this.icmmHelperFacade.getClient().
                    getWorldstate(1, 3, true);
            //*WF* Extract origin schema from WS
//            String originICMMSchema = PilotDHelper.getSchema(originWs);
//            logger.log(Level.INFO, "DB Origin Schema: " + originICMMSchema);
//            logger.log(Level.INFO, "After world state fetching");

            int i = 1;
            int operation = 1;
            for (Integer targetDBWorldStateID : worldStateList) {
                logger.log(Level.FINEST, "Target World State to publish: "
                        + targetDBWorldStateID);

                //*WF* Update CCIM transition object: Preparing workspace for round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Preparing workspace",
                        transition, operation + 1, PROCESS_PHASES, Transition.Status.RUNNING);

//                SimpleFeature eqTDVPar = iterator.next();
                final List<DataItem> resultItems = Lists.<DataItem>newArrayList();

                String worldStateSchemaOnDB = this.icmmHelperFacade.generateWorldStateName(targetDBWorldStateID);
                logger.log(Level.INFO, "Result for world state name creation: " + targetDBWorldStateID);
//                Thread.sleep(30000);
                //*WF* Write target schema dataItem to ICMM
                DataItem schemaItem = this.icmmHelperFacade.writeTargetSchemaDataItem(worldStateSchemaOnDB);
                resultItems.add(schemaItem);

                //*WF* Update CCIM transition object: Running build damage model round round #
                //&& Write transition object to ICMM
                this.icmmHelperFacade.updateTransition("Publishing build damage model round: " + i,
                        transition, operation + 2, PROCESS_PHASES, Transition.Status.RUNNING);

                // execute building damage
                DataStoreInfo crismaDatastore = this.geoServerUtils.getDataStore(crismaWorkspace, worldStateSchemaOnDB);

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
                this.icmmHelperFacade.updateTransition("Publishing building inventory update for round: " + i,
                        transition, operation + 3, PROCESS_PHASES, Transition.Status.RUNNING);

                //*WF* Update the building inventory
//                stringBuilder = new StringBuilder("select aquila.v2_ooi_update('");
//                stringBuilder.append(worldStateSchemaOnDB).append("')");
//                logger.log(Level.INFO, "Building Inventory Query Executed: " + stringBuilder.toString());
//                resultSet = statement.executeQuery(stringBuilder.toString());
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
                this.icmmHelperFacade.updateTransition("Publishing people impact for round: " + i,
                        transition, operation + 4, PROCESS_PHASES, Transition.Status.RUNNING);

                //*WF* Execute people impact procedure
//                stringBuilder = new StringBuilder("select aquila.v2_casualties('");
//                stringBuilder.
//                        append(worldStateSchemaOnDB).
//                        append("',").
//                        append(i).
//                        append(")");
//                resultSet = statement.executeQuery(stringBuilder.toString());
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
                this.icmmHelperFacade.updateTransition("Publishing indicators for round: " + i,
                        transition, operation + 5, PROCESS_PHASES, Transition.Status.RUNNING);

                //TODO: Waiting for procedures to complete the code that calculates the indicators
                Indicators indicators = IndicatorCalculator.calculateIndicators(
                        worldStateSchemaOnDB, connection, false);
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

                operation += 5;
            }
            //*WF* Update CCIM transition object: Status finished
            //&& Write transition object to ICMM
            transition.setDescription("World State Publication, started on "
                    + formatter.format(startDate)
                    + ", ended on " + formatter.format(new Date()));
            this.icmmHelperFacade.updateTransition("Process Executed",
                    transition, PROCESS_PHASES,
                    PROCESS_PHASES, Transition.Status.FINISHED);
            return "Process Executed correctly";
        } catch (Exception e) {
            logger.log(Level.SEVERE, "World State Publication Exception: {0}", e);
            logger.log(Level.SEVERE, "StackTrace: {0}", Arrays.toString(e.getStackTrace()));
            transition.setDescription("WPS World State Publication, started on "
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
