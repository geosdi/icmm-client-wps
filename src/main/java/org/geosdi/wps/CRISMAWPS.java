/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geosdi.wps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class CRISMAWPS implements GeoServerProcess {

    @DescribeProcess(title = "crismaWPS", description = "CRISMA WPS ICMM aware")
    @DescribeResult(name = "result", description = "output result")
    public String execute(@DescribeParameter(name = "name", description = "name to return") String name) {
        return "Hello, " + name;
    }

    @DescribeProcess(title = "Hazard Model", description = "CRISMA WPS for Hazard Model elaboration")
    @DescribeResult(name = "intens grid", description = "WFS link for intensity distribution map")
    public String hazard(@DescribeParameter(name = "isShakemap", description = "Shakemap presence") boolean isShakeMa,
            @DescribeParameter(name = "shakeMapName", description = "Shakemap table name") String shakeMapName,
            @DescribeParameter(name = "latitude", description = "Epi center latitude") Double lat,
            @DescribeParameter(name = "longitude", description = "Epi center longitude") Double lon,
            @DescribeParameter(name = "magnitude", description = "Earthquake magnitude") Double mag) throws Exception{

        Connection conn = this.connectToDatabaseOrDie();

        Statement st = conn.createStatement();
        //select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3);
//        ResultSet rs = st.executeQuery("SELECT * FROM mytable WHERE columnfoo = 500");
        ResultSet rs = st.executeQuery("select aquila.hazard_elaboration(false,'',42.47,13.20,10.0,5.3)");
        while (rs.next()) {
            System.out.print("Column 1 returned ");
            System.out.println(rs.getString(1));
        }
        rs.close();
        st.close();

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
