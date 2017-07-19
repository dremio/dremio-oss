/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

/**
 * Subset of Elasticsearch types for Test Cluster population.
 */
public enum ElasticsearchType {
  BYTE,
  SHORT,
  INTEGER,
  LONG,
  FLOAT,
  DOUBLE,
  BOOLEAN,
  BINARY,
  STRING,
  TEXT,
  KEYWORD,
  DATE,
  IP,
  TIME,
  TIMESTAMP,
  COMPLETION,
  HALF_FLOAT,
  NESTED,
  OBJECT,
  GEO_POINT,
  GEO_SHAPE,
  ATTACHMENT
}
