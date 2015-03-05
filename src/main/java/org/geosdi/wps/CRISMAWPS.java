/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.StyleInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.catalog.impl.DataStoreInfoImpl;
import org.geoserver.catalog.impl.WorkspaceInfoImpl;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.data.DataAccess;
import org.geotools.jdbc.JDBCDataStore;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
@DescribeProcess(title = "Hazard Model", description = "CRISMA WPS for Hazard Model elaboration")
public class CRISMAWPS implements GeoServerProcess {

    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private final static String CRISMA_WORKSPACE = "crisma";
    private final static String CRISMA_DATASTORE = "hazard";

    private Catalog catalog;

    public CRISMAWPS(Catalog catalog) {
        this.catalog = catalog;
    }

//    @DescribeProcess(title = "crismaWPS", description = "CRISMA WPS ICMM aware")
//    @DescribeResult(name = "result", description = "output result")
//    public String execute(@DescribeParameter(name = "name", description = "name to return") String name) {
//        return "Hello, " + name;
//    }
    @DescribeResult(name = "intens grid", description = "WFS link for intensity distribution map")
    public String execute(@DescribeParameter(name = "isShakemap", description = "Shakemap presence") boolean isShakeMa,
            @DescribeParameter(name = "shakeMapName", description = "Shakemap table name") String shakeMapName,
            @DescribeParameter(name = "latitude", description = "Epi center latitude") Double lat,
            @DescribeParameter(name = "longitude", description = "Epi center longitude") Double lon,
            @DescribeParameter(name = "magnitude", description = "Earthquake magnitude") Double mag) throws Exception {

        Connection conn = this.connectToDatabaseOrDie();

        Statement st = conn.createStatement();
        //select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3);
//        ResultSet rs = st.executeQuery("SELECT * FROM mytable WHERE columnfoo = 500");
//        PreparedStatement prepareStatement = conn.prepareStatement("select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3)");
//        st.setInt(1, foovalue);
        ResultSet rs = st.executeQuery("select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3)");
        while (rs.next()) {
            logger.info("Column 1 returned ");
            logger.info("Metadata column count: " + rs.getMetaData().getColumnCount());
            logger.info(rs.getString(1));
        }
        rs.close();

        rs = st.executeQuery("select * from aquila.intens_grid");
        int i = 1;
//        while (rs.next()) {
//            logger.info("Metadata column count: " + rs.getMetaData().getColumnCount());
//            logger.info(rs.getString(i));
//        }
        rs.close();

        st.close();

//        Map<String, Object> params = new HashMap<String, Object>();
//        params.put("dbtype", "postgis");
//        params.put("host", "localhost");
//        params.put("port", 5432);
//        params.put("schema", "public");
//        params.put("database", "crisma");
//        params.put("user", "postgres");
//        params.put("passwd", "0x,postgres,0x");
//
//        DataStore dataStore = DataStoreFinder.getDataStore(params);
//        
//        SimpleFeatureType schema = DataUtilities.createType("Geometry", "centerline:LineString,name:\"\",id:0");
//        dataStore.createSchema(schema);
//
//        FeatureWriter featureWriter = dataStore.getFeatureWriter("Naz", Transaction.AUTO_COMMIT);
//        featureWriter.
        WorkspaceInfo crismaWorkspace = catalog.getWorkspace(CRISMA_WORKSPACE);
        if (crismaWorkspace == null) {
            crismaWorkspace = catalog.getFactory().createWorkspace();
            crismaWorkspace.setName(CRISMA_WORKSPACE);
            catalog.add(crismaWorkspace);
        }
        DataStoreInfo crismaDatastore = catalog.getDataStoreByName(CRISMA_WORKSPACE, CRISMA_DATASTORE);
        if (crismaDatastore == null) {
//            crismaDatastore = catalog.getFactory().createDataStore();
//            crismaDatastore.setName(CRISMA_DATASTORE);
//            crismaDatastore.setWorkspace(crismaWorkspace);
//            crismaDatastore.setType();
//            catalog.add(crismaDatastore);

            DataStoreInfoImpl postgis = new DataStoreInfoImpl(this.catalog);
            postgis.setName(CRISMA_DATASTORE);
            postgis.setType("PostGIS");
            postgis.setEnabled(true);
            postgis.setWorkspace(crismaWorkspace);
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("port", 5432);
            params.put("user", "postgres");
            params.put("passwd", "0x,postgres,0x");
            params.put("dbtype", "postgis");
            params.put("host", "localhost");
            params.put("database", "crisma");
            params.put("namespace", "http://crisma.org");
            params.put("schema", "aquila");
            postgis.setConnectionParameters(params);
            crismaDatastore = postgis;
            catalog.add(crismaDatastore);
        }
        try {
            logger.info("Stores found: " + crismaDatastore.getDataStore(null).getNames().toString());
            DataAccess jdbcStore = (JDBCDataStore) crismaDatastore.getDataStore(null);
        } catch (IOException ioe) {
            logger.warning("Could not initialize postgis db: " + ioe);
        }

        FeatureTypeInfo featureTypeInfo = this.catalog.getFactory().createFeatureType();
        featureTypeInfo.setStore(crismaDatastore);
        featureTypeInfo.setName("intens_grid");
        featureTypeInfo.setNativeName("intens_grid");
        this.catalog.add(featureTypeInfo);

        LayerInfo l = catalog.getFactory().createLayer();
        // l.setName("foo");
        l.setResource(featureTypeInfo);

        StyleInfo s = catalog.getStyleByName("foostyle");
        l.setDefaultStyle(s);
        catalog.add(l);

        //This example issues the same query as before but uses a PreparedStatement and a bind value in the query.
//        int foovalue = 500;
//        PreparedStatement st = conn.prepareStatement("SELECT * FROM mytable WHERE columnfoo = ?");
//        st.setInt(1, foovalue);
//        ResultSet rs = st.executeQuery();
//        while (rs.next()) {
//            System.out.print("Column 1 returned ");
//            System.out.println(rs.getString(1));
//        }
//        rs.close();
//        st.close();
        logger.info("Eseguito processo");
        return "Ekkolo";
    }

    private Connection connectToDatabaseOrDie() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost/crisma";
            conn = DriverManager.getConnection(url, "postgres", "0x,postgres,0x");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
        return conn;
    }

}
