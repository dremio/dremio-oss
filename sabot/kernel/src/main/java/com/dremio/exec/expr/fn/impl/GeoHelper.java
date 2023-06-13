/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.expr.fn.impl;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

import java.util.HashMap;
import java.util.Map;

import com.dremio.exec.expr.fn.FunctionErrorContext;

public class GeoHelper {

  static final long DEFAULT_GEO_HASH_PRECISION = 20;
  static final long GEO_HASH_PRECISION_MIN = 1;
  static final long GEO_HASH_PRECISION_MAX = 20;
  public static final String INVALID_HASH_MSG = "geohash must be a valid, base32-encoded geohash";
  static final Character[] baseHashValues = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

  /**
   * Return a Haversine distance in Kilometers between two points.
   * @param lat1 Lat of point 1
   * @param lon1 Lon of point 1
   * @param lat2 Lat of point 2
   * @param lon2 Lon of point 2
   * @return Distance in miles.
   *
   * From Apache's inbucbator hivemall project.
   * @link http://www.movable-type.co.uk/scripts/latlong.html
   * @link http://rosettacode.org/wiki/Haversine_formula#Java
   * @return distance between two points in Meters
   */
  public static double distance(final double lat1, final double lon1, final double lat2, final double lon2) {
    double R = 6_371_000.0d; // Radius of the earth in m
    double dLat = toRadians(lat2 - lat1); // deg2rad below
    double dLon = toRadians(lon2 - lon1);
    double sinDLat = sin(dLat / 2.d);
    double sinDLon = sin(dLon / 2.d);
    double a = sinDLat * sinDLat + cos(toRadians(lat1)) * cos(toRadians(lat2)) * sinDLon * sinDLon;
    double c = 2.d * atan2(sqrt(a), sqrt(1.d - a));
    return R * c; // Distance in Km
  }
  /**
   *
   * @param lat
   * @param lon
   * @param errCtx
   * @return
   */
  public static String encodeGeohash(final double lat, final double lon, FunctionErrorContext errCtx) {
    return encodeGeohash(lat, lon, DEFAULT_GEO_HASH_PRECISION, errCtx);
  }
  /**
   * Return the GeoHash of a point, with an optional level of precision.
   * The 'official' site (http://geohash.org/site/tips.html) is sparse and points to WikiPedia for documentaion.
   * https://en.wikipedia.org/wiki/Geohash
   * @param lat
   * @param lon
   * @param precision
   * @return String
   */
  public static String encodeGeohash(final double lat, final double lon, final long precision, FunctionErrorContext errCtx) {

    if (precision < GEO_HASH_PRECISION_MIN || precision > GEO_HASH_PRECISION_MAX){
      String errorMessage = "precision must be between " + GEO_HASH_PRECISION_MIN + " and " + GEO_HASH_PRECISION_MAX;
      throw errCtx.error()
        .message(errorMessage)
        .build();
    }
    if (lat < -90.0 || lat > 90.0) {
      throw errCtx.error()
        .message("latitude must be between –90° and +90°")
        .build();
    }
    if (lon < -180.0 || lon > 180.0) {
      throw errCtx.error()
        .message("longitude must be between –180° and +180°")
        .build();
    }

    int dstEnd = 0;
    double latLow = -90.0;
    double latHigh = 90.0;
    double longLow = -180.0;
    double longHigh = 180.0;
    int c = 0;
    int[] evenBits = {16, 8, 4, 2, 1};
    int bit = 0;

    String hash = "";
    int index = 0;
    while (hash.length() < precision) {
      if (index % 2 == 0) {
        //even
        double midValue = (longLow + longHigh) / 2;
        if (lon > midValue) {
          c |= evenBits[bit];
          longLow = midValue;
        } else {
          longHigh = midValue;
        }
      } else {
        //odd
        double midValue = (latLow + latHigh) / 2;
        if (lat > midValue) {
          c |= evenBits[bit];
          latLow = midValue;
        } else {
          latHigh = midValue;
        }
      }
      index++;

      if (bit < 4) {
        bit += 1;
      } else {
        hash += baseHashValues[c];
        bit = 0;
        c = 0;
      }
    }
    return hash;
  }

  /**
   * Decode a geohash into lat, lon values.
   * @param in
   * @param errCtx
   * @return
   */
  public static double[] decodeGeohash(String in, FunctionErrorContext errCtx) {
    Map<Character, Integer> codeMap = buildCodeMap();

    String decoded = "";
    //Only the character values in baseValues are accepted.
    for (int id = 0; id < in.length(); id++) {
      char currentChar = in.charAt(id);
      if ( (Character.isLetter(currentChar) || Character.isDigit(currentChar)) &&
        codeMap.containsKey(currentChar) ) {
        String c = Integer.toBinaryString(codeMap.get(currentChar));
        //Pad to 5 bits.
        while (c.length() < 5) {
          c = '0' + c;
        }
        decoded += c;
      } else {
        throw errCtx.error()
          .message(INVALID_HASH_MSG)
          .build();
      }
    }

    double latLow = -90.0;
    double latHigh = 90.0;
    double lonLow = -180.0;
    double lonHigh = 180.0;

    byte[] decodedBytes = decoded.getBytes();

    for (int i = 0; i < decodedBytes.length; i++) {
      byte b = decodedBytes[i];
      if (i % 2 == 0) {
        if (b == '1') {
          lonLow = (lonLow + lonHigh) / 2;
        } else {
          lonHigh = (lonLow + lonHigh) / 2;
        }
      } else {
        if (b == '1') {
          latLow = (latLow + latHigh) / 2;
        } else {
          latHigh = (latLow + latHigh) / 2;
        }
      }
    }
    double[] result = {(latLow + latHigh) / 2, (lonLow + lonHigh) / 2};
    return result;
  }
  private static Map<Character, Integer> buildCodeMap() {
    Map<Character, Integer> codeMap = new HashMap<>();
    for (int i = 0; i < baseHashValues.length; i++) {
      codeMap.put(baseHashValues[i], i);
    }
    return codeMap;
  }
  /**
   * Given two points, determine if they are within distance of each other.
   *
   * @param lat1     Latitude of point 1
   * @param lon1     Longitude of point 1
   * @param lat2     Latitude of point 2
   * @param lon2     Longitude of point 2
   * @param distance The distance in meters between the two points.
   * @return True if the two points are within the desired distance.
   */
  public static boolean isNear(double lat1, double lon1, double lat2, double lon2, double distance) {
    return distance(lat1, lon1, lat2, lon2) < distance;
  }
}
