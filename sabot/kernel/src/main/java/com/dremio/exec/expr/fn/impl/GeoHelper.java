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

public class GeoHelper {

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
   * Given two points, determine if they are within distance of each other.
   * @param lat1 Latitude of point 1
   * @param lon1 Longitude of point 1
   * @param lat2 Latitude of point 2
   * @param lon2 Longitude of point 2
   * @param distance The distance in meters between the two points.
   * @return True if the two points are within the desired distance.
   */
  public static boolean isNear(double lat1, double lon1, double lat2, double lon2, double distance) {
    return distance(lat1,lon1, lat2, lon2) < distance;
  }

}
