/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import java.text.DecimalFormat;

public class StringToDoubleCodec extends StringToNumberCodec<Double> {

  public StringToDoubleCodec(ThreadLocal<DecimalFormat> formatter) {
    super(cdouble(), formatter);
  }

  @Override
  protected Double convertFrom(String s) {
    return parseAsNumber(s).doubleValue();
  }
}
