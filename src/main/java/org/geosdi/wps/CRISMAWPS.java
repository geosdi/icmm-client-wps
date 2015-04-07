/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geoserver.catalog.AttributeTypeInfo;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CatalogBuilder;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.MetadataMap;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.catalog.impl.DataStoreInfoImpl;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.data.DataAccess;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
@DescribeProcess(title = "Hazard Model", description = "CRISMA WPS for Hazard Model elaboration")
public class CRISMAWPS implements GeoServerProcess {

    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private final static String CRISMA_WORKSPACE = "crisma";
    private final static String CRISMA_DATASTORE = "hazard";

    private Properties properties;
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
        logger.info("Query Executed");
        while (rs.next()) {
            logger.info("Column 1 returned ");
            logger.info("Metadata column count: " + rs.getMetaData().getColumnCount());
            logger.info(rs.getString(1));
        }
        rs.close();

//        rs = st.executeQuery("select * from aquila.intens_grid");
//        int i = 1;
//        while (rs.next()) {
//            logger.info("Metadata column count: " + rs.getMetaData().getColumnCount());
//            logger.info(rs.getString(i));
//        }
//        rs.close();
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
        WorkspaceInfo crismaWorkspace = catalog.getWorkspaceByName(CRISMA_WORKSPACE);
        if (crismaWorkspace == null) {
            crismaWorkspace = catalog.getFactory().createWorkspace();
            crismaWorkspace.setName(CRISMA_WORKSPACE);
            catalog.add(crismaWorkspace);
        }
        //create a namespace corresponding to the workspace if one does not 
        // already exist
        NamespaceInfo namespace = catalog.getNamespaceByPrefix(crismaWorkspace.getName());
        if (namespace == null) {
            logger.info("Automatically creating namespace for workspace " + crismaWorkspace.getName());

            namespace = catalog.getFactory().createNamespace();
            namespace.setPrefix(crismaWorkspace.getName());
            namespace.setURI("http://" + crismaWorkspace.getName());
            catalog.add(namespace);
        }

        DataStoreInfo crismaDatastore = catalog.getDataStoreByName(CRISMA_WORKSPACE, CRISMA_DATASTORE);
        logger.info("crismaDatastore: " + crismaDatastore);
        if (crismaDatastore == null) {
            logger.info("Creating datastore");
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
            params.put("port", properties.getProperty("db.port"));
            params.put("user", properties.getProperty("db.user"));
            params.put("passwd", properties.getProperty("db.passwd"));
            params.put("host", properties.getProperty("db.host"));
            params.put("database", properties.getProperty("db.name"));
            params.put("schema", properties.getProperty("db.schema"));
            params.put("dbtype", "postgis");
            params.put("Loose bbox", "true");
            params.put("Estimated extends", "true");
            params.put("namespace", "http://" + CRISMA_WORKSPACE);
            postgis.setConnectionParameters(params);
            crismaDatastore = postgis;
            catalog.add(crismaDatastore);
        }

        FeatureTypeInfo featureTypeInfo = this.catalog.getFeatureTypeByDataStore(crismaDatastore, "intens_grid");

        logger.info("FeatureTypeInfo: " + featureTypeInfo);

        if (featureTypeInfo == null) {
            logger.info("Creating featureTypeInfo");
            featureTypeInfo = this.catalog.getFactory().createFeatureType();

            featureTypeInfo.setStore(crismaDatastore);
            featureTypeInfo.setNamespace(namespace);
            featureTypeInfo.setName("intens_grid");
            featureTypeInfo.setNativeName("intens_grid");
            logger.info("BUG overriding existing feature type");
//            this.catalog.detach(featureTypeInfo);
            this.catalog.add(featureTypeInfo);
            logger.info("AFTER Creating featureTypeInfo");

            DataAccess gtda = crismaDatastore.getDataStore(null);
            logger.info("crismaDatastore.getDataStore(null): " + crismaDatastore.getDataStore(null));
            if (gtda instanceof DataStore) {
                String typeName = featureTypeInfo.getName();
                if (featureTypeInfo.getNativeName() != null) {
                    typeName = featureTypeInfo.getNativeName();
                }
                boolean typeExists = false;
                DataStore gtds = (DataStore) gtda;
                for (String name : gtds.getTypeNames()) {
                    if (name.equals(typeName)) {
                        typeExists = true;
                        break;
                    }
                }

                //check to see if this is a virtual JDBC feature type
                MetadataMap mdm = featureTypeInfo.getMetadata();
                boolean virtual = mdm != null && mdm.containsKey(FeatureTypeInfo.JDBC_VIRTUAL_TABLE);

                if (!virtual && !typeExists) {
                    gtds.createSchema(buildFeatureType(featureTypeInfo));
                    // the attributes created might not match up 1-1 with the actual spec due to
                    // limitations of the data store, have it re-compute them
                    featureTypeInfo.getAttributes().clear();
                    List<String> typeNames = Arrays.asList(gtds.getTypeNames());
                    // handle Oracle oddities
                    // TODO: use the incoming store capabilites API to better handle the name transformation
                    if (!typeNames.contains(typeName) && typeNames.contains(typeName.toUpperCase())) {
                        featureTypeInfo.setNativeName(featureTypeInfo.getName().toLowerCase());
                    }
                }

                CatalogBuilder cb = new CatalogBuilder(catalog);
                cb.initFeatureType(featureTypeInfo);

                //attempt to fill in metadata from underlying feature source
                try {
                    FeatureSource featureSource
                            = gtda.getFeatureSource(new NameImpl(featureTypeInfo.getNativeName()));
                    if (featureSource != null) {
                        cb.setupMetadata(featureTypeInfo, featureSource);
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING,
                            "Unable to fill in metadata from underlying feature source", e);
                }

                if (featureTypeInfo.getStore() == null) {
                    //get from requests
                    featureTypeInfo.setStore(crismaDatastore);
                }

                NamespaceInfo ns = featureTypeInfo.getNamespace();
                if (ns != null && !ns.getPrefix().equals(crismaWorkspace)) {
                    //TODO: change this once the two can be different and we untie namespace
                    // from workspace
                    logger.warning("Namespace: " + ns.getPrefix() + " does not match workspace: " + crismaWorkspace + ", overriding.");
                    ns = null;
                }

                if (ns == null) {
                    //infer from workspace
                    ns = catalog.getNamespaceByPrefix(CRISMA_WORKSPACE);
                    featureTypeInfo.setNamespace(ns);
                }

                featureTypeInfo.setEnabled(true);
                catalog.validate(featureTypeInfo, true).throwIfInvalid();
                catalog.add(featureTypeInfo);

                //create a layer for the feature type
                catalog.add(new CatalogBuilder(catalog).buildLayer(featureTypeInfo));
            }
        }

//        LayerInfo l = catalog.getFactory().createLayer();
//        // l.setName("foo");
//        l.setResource(featureTypeInfo);
//
//        StyleInfo s = catalog.getStyleByName("foostyle");
//        l.setDefaultStyle(s);
//        catalog.add(l);
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
//        logger.info("Processo Eseguito: " + layer != null ? layer.getPath() : "-");
        return "Processo Eseguito";
    }

    private Connection connectToDatabaseOrDie() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://"
                    + this.properties.getProperty("db.host")
                    + "/" + this.properties.getProperty("db.name");
            conn = DriverManager.getConnection(url,
                    this.properties.getProperty("db.user"),
                    this.properties.getProperty("db.passwd"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
        return conn;
    }

    SimpleFeatureType buildFeatureType(FeatureTypeInfo fti) {
        // basic checks
        if (fti.getName() == null) {
            throw new IllegalArgumentException("Trying to create new feature type inside the store, "
                    + "but no feature type name was specified");
        } else if (fti.getAttributes() == null || fti.getAttributes() == null) {
            throw new IllegalArgumentException("Trying to create new feature type inside the store, "
                    + "but no attributes were specified");
        }

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        if (fti.getNativeName() != null) {
            builder.setName(fti.getNativeName());
        } else {
            builder.setName(fti.getName());
        }
        if (fti.getNativeCRS() != null) {
            builder.setCRS(fti.getNativeCRS());
        } else if (fti.getCRS() != null) {
            builder.setCRS(fti.getCRS());
        } else if (fti.getSRS() != null) {
            builder.setSRS(fti.getSRS());
        }
        for (AttributeTypeInfo ati : fti.getAttributes()) {
            if (ati.getLength() != null && ati.getLength() > 0) {
                builder.length(ati.getLength());
            }
            builder.nillable(ati.isNillable());
            builder.add(ati.getName(), ati.getBinding());
        }
        return builder.buildFeatureType();
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
