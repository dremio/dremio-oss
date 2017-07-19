/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import org.junit.Test;

import static com.dremio.plugins.elastic.ElasticsearchType.ATTACHMENT;

import javax.xml.bind.DatatypeConverter;

/**
 *
 */
public class TestAttachmentType extends ElasticBaseTestQuery {

  @Test
  public void testAttachmentType() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("attachment_field", ATTACHMENT, new Object[][]{
                    {DatatypeConverter.parseBase64Binary("e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=")}
            })
    };

    elastic.load(schema, table, data);

    testBuilder()
            .sqlQuery("select attachment_field from elasticsearch." + schema + "." + table)
            .baselineColumns("attachment_field")
            .unOrdered()
            .baselineValues(DatatypeConverter.parseBase64Binary("e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0="))
            .go();
  }
}
