/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps.utility;

import eu.crismaproject.icmm.icmmhelper.ICMMClient;
import eu.crismaproject.icmm.icmmhelper.ICMMHelper;
import eu.crismaproject.icmm.icmmhelper.entity.Transition;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
import org.geotools.data.DataAccess;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.stereotype.Component;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
@Component
public class Utils {

    public final static String CRISMA_WORKSPACE = "crisma";
    public final static String CRISMA_DATASTORE = "hazard";

    private Logger logger = Logger.getLogger("org.geosdi.wps");

    private Properties properties;

    private final ICMMClient client;
    private final Catalog catalog;

    public Utils(Catalog catalog, String icmm_url) {
        this.catalog = catalog;
        this.client = new ICMMClient(icmm_url);
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
        return transition;
    }

    public WorkspaceInfo getWorkspace() {
        WorkspaceInfo workspaceInfo = catalog.getWorkspaceByName(Utils.CRISMA_WORKSPACE);
        if (workspaceInfo == null) {
            workspaceInfo = catalog.getFactory().createWorkspace();
            workspaceInfo.setName(Utils.CRISMA_WORKSPACE);
            catalog.add(workspaceInfo);
        }
        return workspaceInfo;
    }

    public NamespaceInfo getNamespace(WorkspaceInfo crismaWorkspace) {
        NamespaceInfo namespace = catalog.getNamespaceByPrefix(crismaWorkspace.getName());
        if (namespace == null) {
            logger.info("Automatically creating namespace for workspace " + crismaWorkspace.getName());

            namespace = catalog.getFactory().createNamespace();
            namespace.setPrefix(crismaWorkspace.getName());
            namespace.setURI("http://" + crismaWorkspace.getName());
            catalog.add(namespace);
        }
        return namespace;
    }

    public DataStoreInfo getDataStore(WorkspaceInfo crismaWorkspace) {
        DataStoreInfo crismaDatastore = catalog.getDataStoreByName(
                Utils.CRISMA_WORKSPACE, Utils.CRISMA_DATASTORE);
        logger.info("crismaDatastore: " + crismaDatastore);
        if (crismaDatastore == null) {
            logger.info("Creating datastore");
            DataStoreInfoImpl postgis = new DataStoreInfoImpl(this.catalog);
            postgis.setName(Utils.CRISMA_DATASTORE);
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
            params.put("namespace", "http://" + Utils.CRISMA_WORKSPACE);
            postgis.setConnectionParameters(params);
            crismaDatastore = postgis;
            catalog.add(crismaDatastore);
        }
        return crismaDatastore;
    }

    public FeatureTypeInfo getFeatureType(WorkspaceInfo crismaWorkspace,
            DataStoreInfo crismaDatastore, NamespaceInfo namespace) throws Exception {
        FeatureTypeInfo featureTypeInfo = this.catalog.getFeatureTypeByDataStore(
                crismaDatastore, "intens_grid");

        LayerInfo layer = this.catalog.getLayerByName("intens_grid");

        if (featureTypeInfo != null && layer == null) {
            this.catalog.remove(featureTypeInfo);
            featureTypeInfo = null;
        }

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
                    logger.warning("Namespace: " + ns.getPrefix()
                            + " does not match workspace: " + crismaWorkspace + ", overriding.");
                    ns = null;
                }

                if (ns == null) {
                    //infer from workspace
                    ns = catalog.getNamespaceByPrefix(Utils.CRISMA_WORKSPACE);
                    featureTypeInfo.setNamespace(ns);
                }

                featureTypeInfo.setEnabled(true);
                catalog.validate(featureTypeInfo, true).throwIfInvalid();
                catalog.add(featureTypeInfo);

                //create a layer for the feature type
                catalog.add(new CatalogBuilder(catalog).buildLayer(featureTypeInfo));
            }
        }
        return featureTypeInfo;
    }

    public void updateTransition(String message, Transition transition, int runningPhase,
            int processStepsNumber, Transition.Status status) {
        ICMMHelper.updateTransition(transition, status, runningPhase, 
                processStepsNumber, message);
        client.putTransition(transition);
    }

    public Connection connectToDatabaseOrDie() {
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

    private SimpleFeatureType buildFeatureType(FeatureTypeInfo fti) {
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

    public ICMMClient getClient() {
        return client;
    }

}
