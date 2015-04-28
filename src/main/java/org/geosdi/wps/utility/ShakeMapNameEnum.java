/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.geosdi.wps.utility;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public enum ShakeMapNameEnum {

    shakemap_1("grid_xyz_20090406"), shakemap_2("grid_xyz_20090406_332"), 
    shakemap_3("grid_xyz_20090409"), shakemap_4("grid_xyz_20090622");
    
    private String name;

    private ShakeMapNameEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    
}
