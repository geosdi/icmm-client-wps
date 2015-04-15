/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geoserver.wps.ppio;

import org.geoserver.wps.resource.WPSResourceManager;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class EqTDVPar extends RawDataPPIO {

    private static final long serialVersionUID = -5043335389756597349L;

    private boolean useShakemap;
    private String shakeMapName;
    private Long latitude;
    private Long longitude;
    private Long magnitude;
    private Long depth;

    public EqTDVPar(WPSResourceManager resourceManager) {
        super(resourceManager);
    }

//    public EqTDVPar(Class externalType, Class internalType, String mimeType) {
//        super(externalType, internalType, mimeType);
//    }
    public boolean isUseShakemap() {
        return useShakemap;
    }

    public void setUseShakemap(boolean useShakemap) {
        this.useShakemap = useShakemap;
    }

    public String getShakeMapName() {
        return shakeMapName;
    }

    public void setShakeMapName(String shakeMapName) {
        this.shakeMapName = shakeMapName;
    }

    public Long getLatitude() {
        return latitude;
    }

    public void setLatitude(Long latitude) {
        this.latitude = latitude;
    }

    public Long getLongitude() {
        return longitude;
    }

    public void setLongitude(Long longitude) {
        this.longitude = longitude;
    }

    public Long getMagnitude() {
        return magnitude;
    }

    public void setMagnitude(Long magnitude) {
        this.magnitude = magnitude;
    }

    public Long getDepth() {
        return depth;
    }

    public void setDepth(Long depth) {
        this.depth = depth;
    }

    @Override
    public String toString() {
        return "EqTDVPar{" + "useShakemap=" + useShakemap + ", shakeMapName="
                + shakeMapName + ", latitude=" + latitude + ", longitude="
                + longitude + ", magnitude=" + magnitude + ", depth=" + depth + '}';
    }

}
