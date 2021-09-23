/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

public class H3TraversalFunctions {

  @FunctionTemplate(names = {"kRing"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class kRing implements DrillSimpleFunc {

    @Param
    BigIntHolder originInput;

    @Param
    IntHolder kInput;

    @Output
    BaseWriter.ComplexWriter outWriter;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Override
    public void setup() {
      try {
        h3 = com.uber.h3core.H3Core.newInstance();
      } catch (java.io.IOException e) {
        h3 = null;
      }
    }

    @Override
    public void eval() {
      if (h3 == null) {
        return;
      }

      long origin = originInput.value;
      int k = kInput.value;

      java.util.List<Long> results = h3.kRing(origin, k);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();

      for (Long result : results) {
        queryListWriter.bigInt().writeBigInt(result);
      }
    }
  }

  @FunctionTemplate(names = {"kRing"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class kRingFromString implements DrillSimpleFunc {

    @Param
    VarCharHolder originInput;

    @Param
    IntHolder kInput;

    @Output
    BaseWriter.ComplexWriter outWriter;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Override
    public void setup() {
      try {
        h3 = com.uber.h3core.H3Core.newInstance();
      } catch (java.io.IOException e) {
        h3 = null;
      }
    }

    @Override
    public void eval() {
      if (h3 == null) {
        return;
      }

      String h3Address = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(originInput);
      int k = kInput.value;

      java.util.List<String> results = h3.kRing(h3Address, k);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      for (String result : results) {
        buffer.setBytes(0, result.getBytes());
        queryListWriter.varChar().writeVarChar(0, result.getBytes().length, buffer);
      }
    }
  }

  @FunctionTemplate(names = {"kRingDistances"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class kRingDistances implements DrillSimpleFunc {

    @Param
    BigIntHolder originInput;

    @Param
    IntHolder kInput;

    @Output
    BaseWriter.ComplexWriter outWriter;

    @Workspace
    com.uber.h3core.H3Core h3;

    @Override
    public void setup() {
      try {
        h3 = com.uber.h3core.H3Core.newInstance();
      } catch (java.io.IOException e) {
        h3 = null;
      }
    }

    @Override
    public void eval() {
      if (h3 == null) {
        return;
      }

      long origin = originInput.value;
      int k = kInput.value;

      java.util.List<java.util.List<Long>> results = h3.kRingDistances(origin, k);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter innerListWriter;
      for (java.util.List<Long> innerList : results) {
        innerListWriter = queryListWriter.list();
        innerListWriter.startList();
        for (Long result: innerList) {
          innerListWriter.bigInt().writeBigInt(result);
        }
        innerListWriter.endList();
      }
    }
  }
}
