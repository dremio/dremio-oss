/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_SHAPE;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for the elasticsearch geo_shape data type.
 */
public class TestGeoShapeType extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestGeoShapeType.class);

  @Test
  public void testGeoShape_Circle() throws Exception {

    double[] point = {-45.0, 45.0};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "circle", "radius", "100m", "coordinates", point)},
                    {ImmutableMap.of("type", "circle", "radius", "10m", "coordinates", point)},
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(mapOf("type", "circle",
                    "radius", "10m",
                    "coordinates", listOf(point[1], point[0])))
            .baselineValues(mapOf("type", "circle",
                    "radius", "100m",
                    "coordinates", listOf(point[1], point[0])))
            .go();
  }

  @Test
  public void testGeoShape_Envelope() throws Exception {

    double[][] points = {{-45.0, 45.0}, {45.0, -45.0}};
    double[][] points2 = {{-90.0, 90.0}, {90.0, -90.0}};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "envelope", "coordinates", points)},
                    {ImmutableMap.of("type", "envelope", "coordinates", points2)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "envelope", "coordinates",
                            listOf(
                                    listOf(points[0][1], points[0][0]),
                                    listOf(points[1][1], points[1][0]))))
            .baselineValues(
                    mapOf("type", "envelope", "coordinates",
                            listOf(
                                    listOf(points2[0][1], points2[0][0]),
                                    listOf(points2[1][1], points2[1][0]))))
            .go();
  }

  @Test
  public void testGeoShape_GeometryCollection() throws Exception {

    double[] point1 = {100.0, 0.0};
    double[] point2 = {101.0, 5.0};

    // XXX - Pending DX-290
    double[][] line = {{101.0, 0.0}, {102.0, 1.0}};

    List<Object> inputEmpty = new ArrayList<>();
    List<Object> input = new ArrayList<>();

    input.add(ImmutableMap.of("type", "point", "coordinates", point1));
    input.add(ImmutableMap.of("type", "point", "coordinates", point2));

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "geometrycollection", "geometries", input)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "geometrycollection", "geometries",
                            listOf(
                                    mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                                    mapOf("type", "point", "coordinates", listOf(point2[1], point2[0])))))
            .go();
  }

  @Test
  @Ignore("DX-7868")
  public void testGeoShape_ArrayOfGeometryCollection() throws Exception {

    double[] point1 = {100.0, 1.0};
    double[] point2 = {101.0, 1.0};
    double[] point3 = {100.0, 2.0};
    double[] point4 = {101.0, 2.0};
    double[] point5 = {100.0, 3.0};
    double[] point6 = {101.0, 3.0};

    List<Object> input1 = new ArrayList<>();
    List<Object> input2 = new ArrayList<>();
    List<Object> input3 = new ArrayList<>();

    input1.add(ImmutableMap.of("type", "point", "coordinates", point1));
    input1.add(ImmutableMap.of("type", "point", "coordinates", point2));

    input2.add(ImmutableMap.of("type", "point", "coordinates", point3));
    input2.add(ImmutableMap.of("type", "point", "coordinates", point4));

    input3.add(ImmutableMap.of("type", "point", "coordinates", point5));
    input3.add(ImmutableMap.of("type", "point", "coordinates", point6));

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input1),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input1)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2)
                    }
            })
    };

    elastic.load(schema, table, data);


    logger.info("search:\n{}", elastic.search(schema, table));

    String sql = "select location_field from elasticsearch." + schema + "." + table;

    //testRunAndPrint(UserBitShared.QueryType.SQL, sql);

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                                            mapOf("type", "point", "coordinates", listOf(point2[1], point2[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                                            mapOf("type", "point", "coordinates", listOf(point2[1], point2[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0]))))))
            .go();
  }

  @Test
  public void testGeoShape_PolygonWithHole() throws Exception {

    // Polygons can have holes, e.g. like a donut.

    double[] point1 = {100.0, 0.0};
    double[] point2 = {101.0, 0.0};
    double[] point3 = {101.0, 1.0};
    double[] point4 = {100.0, 1.0};
    double[] point5 = {100.0, 0.0};

    double[] innerPoint1 = {100.2, 0.2};
    double[] innerPoint2 = {100.8, 0.2};
    double[] innerPoint3 = {100.8, 0.8};
    double[] innerPoint4 = {100.2, 0.8};
    double[] innerPoint5 = {100.2, 0.2};

    double[][][] polygon = new double[][][]{
            {point1, point2, point3, point4, point5},
            {innerPoint1, innerPoint2, innerPoint3, innerPoint4, innerPoint5}
    };

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "polygon", "coordinates", polygon)}
            })
    };

    elastic.load(schema, table, data);


        /*
            "location_field":
            {
                "type":"polygon","coordinates":
                    [   [   [100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0] ],
                        [   [100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2] ]
                    ]
            }
         */

    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "polygon", "coordinates",
                            listOf(
                                    listOf(
                                            listOf(point1[1], point1[0]),
                                            listOf(point2[1], point2[0]),
                                            listOf(point3[1], point3[0]),
                                            listOf(point4[1], point4[0]),
                                            listOf(point5[1], point5[0])
                                    ),
                                    listOf(
                                            listOf(innerPoint1[1], innerPoint1[0]),
                                            listOf(innerPoint2[1], innerPoint2[0]),
                                            listOf(innerPoint3[1], innerPoint3[0]),
                                            listOf(innerPoint4[1], innerPoint4[0]),
                                            listOf(innerPoint5[1], innerPoint5[0])))))
            .go();
  }

  @Test
  public void testGeoShape_Polygon() throws Exception {

    double[] point1 = {100.0, 0.0};
    double[] point2 = {101.0, 0.0};
    double[] point3 = {101.0, 1.0};
    double[] point4 = {100.0, 1.0};
    double[] point5 = {100.0, 0.0};

    double[][][] polygon = new double[][][]{
            {point1, point2, point3, point4, point5}
    };

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "polygon", "coordinates", polygon)}
            })
    };

    elastic.load(schema, table, data);


    // Polygons are encoded by elasticsearch as a list of a list of points.

    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "polygon", "coordinates",
                            listOf(
                                    listOf(
                                            listOf(point1[1], point1[0]),
                                            listOf(point2[1], point2[0]),
                                            listOf(point3[1], point3[0]),
                                            listOf(point4[1], point4[0]),
                                            listOf(point5[1], point5[0]))
                            )))
            .go();
  }

  @Test
  public void testGeoShape_MultiPolygon() throws Exception {

    double[] point1 = {100.0, 0.0};
    double[] point2 = {101.0, 0.0};
    double[] point3 = {101.0, 1.0};
    double[] point4 = {100.0, 1.0};
    double[] point5 = {100.0, 0.0};

    double[] point6 = {100.2, 0.2};
    double[] point7 = {100.8, 0.2};
    double[] point8 = {100.8, 0.8};
    double[] point9 = {100.2, 0.8};
    double[] point10 = {100.2, 0.2};

    double[][][] polygon1 = new double[][][]{{point1, point2, point3, point4, point5}};
    double[][][] polygon2 = new double[][][]{{point6, point7, point8, point9, point10}};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "multipolygon", "coordinates",
                            new double[][][][]{polygon1, polygon2})}
            })
    };

    elastic.load(schema, table, data);


        /*
            "location_field":
                {"type":"multipolygon",
                "coordinates":
                    [
                        [
                            [ [100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0] ]
                        ],
                        [
                            [ [100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2] ]
                        ]
                    ]}
         */

    String sql = "select location_field from elasticsearch." + schema + "." + table;

    testRunAndPrint(UserBitShared.QueryType.SQL, sql);

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "multipolygon", "coordinates",
                            listOf(
                                    listOf(
                                            listOf(
                                                    listOf(point1[1], point1[0]),
                                                    listOf(point2[1], point2[0]),
                                                    listOf(point3[1], point3[0]),
                                                    listOf(point4[1], point4[0]),
                                                    listOf(point5[1], point5[0]))),
                                    listOf(
                                            listOf(
                                                    listOf(point6[1], point6[0]),
                                                    listOf(point7[1], point7[0]),
                                                    listOf(point8[1], point8[0]),
                                                    listOf(point9[1], point9[0]),
                                                    listOf(point10[1], point10[0])))
                            )))
            .go();
  }

  @Test
  public void testGeoShape_LineString() throws Exception {

    double[] point1 = {-77.03653, 38.897676};
    double[] point2 = {-77.009051, 38.889939};
    double[] point3 = {-1.03653, 30.897676};
    double[] point4 = {-101.009051, 35.889939};

    double[][] line1 = {point1, point2, point3, point4};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "linestring", "coordinates", line1)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "linestring", "coordinates",
                            listOf(
                                    listOf(point1[1], point1[0]),
                                    listOf(point2[1], point2[0]),
                                    listOf(point3[1], point3[0]),
                                    listOf(point4[1], point4[0]))))
            .go();
  }

  @Test
  public void testGeoShape_MultiLineString() throws Exception {

    double[][] line1 = {{102.0, 2.0}, {103.0, 2.0}, {103.0, 3.0}, {102.0, 3.0}};
    double[][] line2 = {{100.0, 0.0}, {101.0, 0.0}, {101.0, 1.0}, {100.0, 1.0}};
    double[][] line3 = {{100.2, 0.2}, {100.8, 0.2}, {100.8, 0.8}, {100.2, 0.8}};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "multilinestring", "coordinates",
                            new double[][][]{
                                    line1, line2, line3
                            })}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "multilinestring", "coordinates",
                            listOf(
                                    listOf(
                                            listOf(line1[0][1], line1[0][0]),
                                            listOf(line1[1][1], line1[1][0]),
                                            listOf(line1[2][1], line1[2][0]),
                                            listOf(line1[3][1], line1[3][0])
                                    ),
                                    listOf(
                                            listOf(line2[0][1], line2[0][0]),
                                            listOf(line2[1][1], line2[1][0]),
                                            listOf(line2[2][1], line2[2][0]),
                                            listOf(line2[3][1], line2[3][0])
                                    ),
                                    listOf(
                                            listOf(line3[0][1], line3[0][0]),
                                            listOf(line3[1][1], line3[1][0]),
                                            listOf(line3[2][1], line3[2][0]),
                                            listOf(line3[3][1], line3[3][0])
                                    ))))
            .go();
  }

  @Test
  public void testGeoShape_SinglePoint() throws Exception {

    double[] point1 = new double[]{-77.03653, 38.897676};
    double[] point2 = new double[]{-78.03653, 39.897676};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "point", "coordinates", point1)},
                    {ImmutableMap.of("type", "point", "coordinates", point2)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])))
            .baselineValues(
                    mapOf("type", "point", "coordinates", listOf(point2[1], point2[0])))
            .go();
  }

  @Test
  public void testGeoShape_MultiPoint() throws Exception {

    double[] point1 = {102.0, 2.0};
    double[] point2 = {103.0, 2.0};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    {ImmutableMap.of("type", "multipoint", "coordinates", new double[][]{point1, point2})}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "multipoint", "coordinates",
                            listOf(
                                    listOf(point1[1], point1[0]),
                                    listOf(point2[1], point2[0])
                            )))
            .go();
  }

  @Test
  @Ignore("DX-7868")
  public void testGeoShape_ArrayOfSinglePoint() throws Exception {

    double[] point1 = new double[]{-77.03653, 38.897676};
    double[] point2 = new double[]{-78.03653, 39.897676};
    double[] point3 = new double[]{-16.0353, 4.8676};

    double[] point4 = new double[]{77.03653, -38.897676};
    double[] point5 = new double[]{78.03653, -39.897676};
    double[] point6 = new double[]{16.0353, -4.8676};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{

                    new Object[]{
                            ImmutableMap.of("type", "point", "coordinates", point1),
                            ImmutableMap.of("type", "point", "coordinates", point2),
                            ImmutableMap.of("type", "point", "coordinates", point3)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "point", "coordinates", point4),
                            ImmutableMap.of("type", "point", "coordinates", point5),
                            ImmutableMap.of("type", "point", "coordinates", point6)
                    }
            })
    };

    elastic.load(schema, table, data);


    String sql = "select location_field from elasticsearch." + schema + "." + table;

    testRunAndPrint(UserBitShared.QueryType.SQL, sql);

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    listOf(
                            mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                            mapOf("type", "point", "coordinates", listOf(point2[1], point2[0])),
                            mapOf("type", "point", "coordinates", listOf(point3[1], point3[0]))
                    ))
            .baselineValues(
                    listOf(
                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])),
                            mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0]))
                    ))
            .go();
  }

  @Test
  public void testGeoShape_DifferentShapesSameColumn() throws Exception {

    double[] point1 = new double[]{-77.03653, 38.897676};
    double[] circleCenter = {-45.0, 45.0};

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    // a point
                    {ImmutableMap.of("type", "point", "coordinates", point1)},
                    // a circle
                    {ImmutableMap.of("type", "circle", "radius", "10m", "coordinates", circleCenter)},
            })
    };
    elastic.load(schema, table, data);


    String sql = "select location_field from elasticsearch." + schema + "." + table;

    testRunAndPrint(UserBitShared.QueryType.SQL, sql);

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    mapOf("type", "point", "coordinates", listOf(point1[1], point1[0]))
            )
            .baselineValues(
                    mapOf("type", "circle", "radius", "10m", "coordinates", listOf(circleCenter[1], circleCenter[0]))
            )
            .go();

  }


  @Test
  @Ignore("Until DX-290 is fixed")
  public void testGeoShape_Union() throws Exception {

    double[] point1 = {100.0, 1.0};
    double[] point2 = {101.0, 1.0};
    double[] point3 = {100.0, 2.0};
    double[] point4 = {101.0, 2.0};
    double[] point5 = {100.0, 3.0};
    double[] point6 = {101.0, 3.0};

    List<Object> input1 = new ArrayList<>();
    List<Object> input2 = new ArrayList<>();
    List<Object> input3 = new ArrayList<>();

    input1.add(ImmutableMap.of("type", "point", "coordinates", point1));
    input1.add(ImmutableMap.of("type", "point", "coordinates", point2));

    input2.add(ImmutableMap.of("type", "point", "coordinates", point3));
    input2.add(ImmutableMap.of("type", "point", "coordinates", point4));

    input3.add(ImmutableMap.of("type", "point", "coordinates", point5));
    input3.add(ImmutableMap.of("type", "point", "coordinates", point6));

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_SHAPE, ImmutableMap.of("tree", "quadtree"), new Object[][]{
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input1),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input1)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input3)
                    },
                    new Object[]{
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2),
                            ImmutableMap.of("type", "geometrycollection", "geometries", input2)
                    }
            })
    };

    elastic.load(schema, table, data);


    logger.info("search:\n{}", elastic.search(schema, table));

    String sql = "select location_field from elasticsearch." + schema + "." + table;

    //testRunAndPrint(UserBitShared.QueryType.SQL, sql);

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("location_field")
            .unOrdered()
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                                            mapOf("type", "point", "coordinates", listOf(point2[1], point2[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point1[1], point1[0])),
                                            mapOf("type", "point", "coordinates", listOf(point2[1], point2[0]))))))
            .baselineValues(
                    mapOf("type", "geometrycollection", "geometries",
                            listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                    mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point5[1], point5[0])),
                                            mapOf("type", "point", "coordinates", listOf(point6[1], point6[0]))))))
            .baselineValues(
                    listOf(
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0])))),
                            mapOf("type", "geometrycollection", "geometries",
                                    listOf(mapOf("type", "point", "coordinates", listOf(point3[1], point3[0])),
                                            mapOf("type", "point", "coordinates", listOf(point4[1], point4[0]))))))
            .go();
  }


}
