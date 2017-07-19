/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.google.common.collect.ImmutableMap;

import static com.dremio.plugins.elastic.ElasticsearchType.GEO_POINT;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests for the elasticsearch geo_point data type.
 */
public class TestGeoPointType extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestGeoPointType.class);

  @Test
  public void testSelectGeoPointField() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_POINT, new Object[][]{
                    {ImmutableMap.of("lat", 1.2, "lon", 2.5)},
                    {ImmutableMap.of("lat", 35.67, "lon", -12.2)}
            })
    };

    elastic.load(schema, table, data);

    testBuilder()
            .sqlQuery("select location_field from elasticsearch." + schema + "." + table)
            .unOrdered()
            .baselineColumns("location_field")
            .baselineValues(ImmutableMap.of("lat", 1.2, "lon", 2.5))
            .baselineValues(ImmutableMap.of("lat", 35.67, "lon", -12.2))
            .go();
  }

  @Test
  public void testSelectLatitudeFromGeoPointField() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_POINT, new Object[][]{
                    {ImmutableMap.of("lat", 1.2, "lon", 2.5)},
                    {ImmutableMap.of("lat", 35.67, "lon", -12.2)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select t.location_field.lat as latitude from elasticsearch." + schema + "." + table + " t")
            .unOrdered()
            .baselineColumns("latitude")
            .baselineValues(1.2)
            .baselineValues(35.67)
            .go();
  }

  @Test
  public void testSelectLongitudeFromGeoPointField() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_POINT, new Object[][]{
                    {ImmutableMap.of("lat", 1.2, "lon", 2.5)},
                    {ImmutableMap.of("lat", 35.67, "lon", -12.2)}
            })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select t.location_field.lon as longitude from elasticsearch." + schema + "." + table + " t")
            .unOrdered()
            .baselineColumns("longitude")
            .baselineValues(2.5)
            .baselineValues(-12.2)
            .go();
  }

  @Test
  public void testSelectArrayOfGeoPointField() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("location_field", GEO_POINT, new Object[][]{
                    {ImmutableMap.of("lat", 42.1, "lon", -31.66), ImmutableMap.of("lat", 35.6, "lon", -42.1)},
                    {ImmutableMap.of("lat", 23.1, "lon", -23.01), ImmutableMap.of("lat", -23.0, "lon", 9)}
            })
    };

    elastic.load(schema, table, data);


    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> search:\n{}", elastic.search(schema, table));

    testRunAndPrint(UserBitShared.QueryType.SQL, "select t.location_field from elasticsearch." + schema + "." + table + " t");

    testBuilder()
            .sqlQuery("select t.location_field[1].lat as lat_1 from elasticsearch." + schema + "." + table + " t")
            .unOrdered()
            .baselineColumns("lat_1")
            .baselineValues(35.6)
            .baselineValues(-23.0)
            .go();
  }
}
