package com.vpon.wizad.etl.util;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Location;

public class GenerateQuadKey {
    LookupService lookupService;

    public final static int GEOIP_STANDARD = 0;
    public final static int GEOIP_MEMORY_CACHE = 1;
    public final static int GEOIP_CHECK_CACHE = 2;
    public final static int GEOIP_INDEX_CACHE = 4;
    public final static int GEOIP_UNKNOWN_SPEED = 0;
    public final static int GEOIP_DIALUP_SPEED = 1;
    public final static int GEOIP_CABLEDSL_SPEED = 2;
    public final static int GEOIP_CORPORATE_SPEED = 3;

    private static final double MinLatitude = -85.05112878;
    private static final double MaxLatitude = 85.05112878;
    private static final double MinLongitude = -180;
    private static final double MaxLongitude = 180;

    static class IntCoord {
        public int x;
        public int y;
    }

    private static double Clip(double n, double minValue, double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    public static IntCoord LatLongToPixelXY(double latitude, double longitude, int levelOfDetail) {
        latitude = Clip(latitude, MinLatitude, MaxLatitude);
        longitude = Clip(longitude, MinLongitude, MaxLongitude);

        double x = (longitude + 180) / 360;
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        long mapSize = MapSize(levelOfDetail);

        IntCoord pc = new IntCoord();
        pc.x = (int) Clip(x * mapSize + 0.5, 0, mapSize - 1);
        pc.y = (int) Clip(y * mapSize + 0.5, 0, mapSize - 1);
        return pc;
    }

    public static long MapSize(int levelOfDetail) {
        return (long) 256 << levelOfDetail;
    }

    public static IntCoord PixelXYToTileXY(IntCoord pixelCoord) {
        return PixelXYToTileXY(pixelCoord.x, pixelCoord.y);
    }

    public static IntCoord PixelXYToTileXY(int pixelX, int pixelY) {
        IntCoord tileCoord = new IntCoord();
        tileCoord.x = pixelX / 256;
        tileCoord.y = pixelY / 256;
        return tileCoord;
    }

    public static String TileXYToQuadKey(IntCoord tileCoord, int levelOfDetail) {
        return TileXYToQuadKey(tileCoord.x, tileCoord.y, levelOfDetail);
    }

    public static String TileXYToQuadKey(int tileX, int tileY, int levelOfDetail) {
        StringBuilder quadKey = new StringBuilder();
        for (int i = levelOfDetail; i > 0; i--) {
            char digit = '0';
            int mask = 1 << (i - 1);
            if ((tileX & mask) != 0) {
                digit++;
            }
            if ((tileY & mask) != 0) {
                digit++;
                digit++;
            }
            quadKey.append(digit);
        }
        return quadKey.toString();
    }

    public String generateQuadKey(double latitude, double longitude) {
        IntCoord pc = LatLongToPixelXY(latitude, longitude, 15);
        IntCoord pc2 = PixelXYToTileXY(pc);
        String qk = TileXYToQuadKey(pc2, 15);
        return qk;
    }

    public String generateQuadKey(String ip) throws Exception {
        if (lookupService == null) {
            lookupService = new LookupService("./GeoIPCityap.dat", LookupService.GEOIP_MEMORY_CACHE);
        }
        Location location = lookupService.getLocation(ip);

        double latitude = location.latitude;
        double longitude = location.longitude;

        IntCoord pc = LatLongToPixelXY(latitude, longitude, 15);
        IntCoord pc2 = PixelXYToTileXY(pc);

        String qk = TileXYToQuadKey(pc2, 15);
        return qk;
    }
}




