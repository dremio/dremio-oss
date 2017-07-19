/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import static java.lang.String.format;

/**
 * Indicates a data type mapping was encountered in an elasticsearch index that
 * is not supported in the Dremio elasticsearch storage plugin.
 */
public class UnsupportedTypeException extends Exception {

  private final String field;
  private final String type;

  public UnsupportedTypeException(final String field, final String type) {
    this.field = field;
    this.type = type;
  }

  @Override
  public String getMessage() {
    return format("Field [%s] declares an unsupported data type: [%s]", field, type);
  }
}
