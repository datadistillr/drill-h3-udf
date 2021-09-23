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
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
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

  @FunctionTemplate(names = {"kRingDistances"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class kRingStringDistances implements DrillSimpleFunc {

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

      java.util.List<java.util.List<String>> results = h3.kRingDistances(h3Address, k);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter innerListWriter;
      for (java.util.List<String> innerList : results) {
        innerListWriter = queryListWriter.list();
        innerListWriter.startList();
        for (String result: innerList) {
          buffer.setBytes(0, result.getBytes());
          innerListWriter.varChar().writeVarChar(0, result.length(), buffer);
        }
        innerListWriter.endList();
      }
    }
  }


  @FunctionTemplate(names = {"hexRange", "hex_range"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class hexRange implements DrillSimpleFunc {

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
      java.util.List<java.util.List<Long>> results;
      try {
        results = h3.hexRange(origin, k);
      } catch (com.uber.h3core.exceptions.PentagonEncounteredException e) {
        return;
      }
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

  @FunctionTemplate(names = {"hexRange", "hex_range"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class hexRangeString implements DrillSimpleFunc {

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
      java.util.List<java.util.List<String>> results;
      try {
        results = h3.hexRange(h3Address, k);
      } catch (com.uber.h3core.exceptions.PentagonEncounteredException e) {
        return;
      }

      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter innerListWriter;

      for (java.util.List<String> innerList : results) {
        innerListWriter = queryListWriter.list();
        innerListWriter.startList();
        for (String result: innerList) {
          buffer.setBytes(0, result.getBytes());
          innerListWriter.varChar().writeVarChar(0, result.length(), buffer);
        }
        innerListWriter.endList();
      }
    }
  }

  @FunctionTemplate(names = {"hexRing", "hex_ring"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class hexRing implements DrillSimpleFunc {

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

      java.util.List<Long> results = null;
      try {
        results = h3.hexRing(origin, k);
      } catch (com.uber.h3core.exceptions.PentagonEncounteredException e) {
        return;
      }
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();

      for (Long result : results) {
        queryListWriter.bigInt().writeBigInt(result);
      }
    }
  }

  @FunctionTemplate(names = {"hexRing", "hex_ring"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class hexRingFromString implements DrillSimpleFunc {

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

      java.util.List<String> results = null;
      try {
        results = h3.hexRing(h3Address, k);
      } catch (com.uber.h3core.exceptions.PentagonEncounteredException e) {
        return;
      }
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      for (String result : results) {
        buffer.setBytes(0, result.getBytes());
        queryListWriter.varChar().writeVarChar(0, result.getBytes().length, buffer);
      }
    }
  }

  @FunctionTemplate(names = {"h3Line", "h3_line"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class h3Line implements DrillSimpleFunc {

    @Param
    BigIntHolder startHolder;

    @Param
    BigIntHolder endHolder;

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

      long start = startHolder.value;
      long end = endHolder.value;
      java.util.List<Long> line;
      try {
       line = h3.h3Line(start, end);
      } catch (com.uber.h3core.exceptions.LineUndefinedException e) {
        return;
      }

      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();

      for (Long result : line) {
        queryListWriter.bigInt().writeBigInt(result);
      }
    }
  }


  @FunctionTemplate(names = {"h3Line", "h3_line"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class h3StringLine implements DrillSimpleFunc {

    @Param
    VarCharHolder startHolder;

    @Param
    VarCharHolder endHolder;

    @Inject
    DrillBuf buffer;

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

      String start = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(startHolder);
      String end = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(endHolder);
      java.util.List<String> line;
      try {
        line = h3.h3Line(start, end);
      } catch (com.uber.h3core.exceptions.LineUndefinedException e) {
        return;
      }

      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter queryListWriter = outWriter.rootAsList();
      for (String result : line) {
        buffer.setBytes(0, result.getBytes());
        queryListWriter.varChar().writeVarChar(0, result.getBytes().length, buffer);
      }
    }
  }

  @FunctionTemplate(names = {"h3Distance", "h3_distance"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class h3Distance implements DrillSimpleFunc {

    @Param
    BigIntHolder startHolder;

    @Param
    BigIntHolder endHolder;

    @Output
    BigIntHolder out;

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

      long start = startHolder.value;
      long end = endHolder.value;
      int distance;
      try {
        distance = h3.h3Distance(start, end);
      } catch (com.uber.h3core.exceptions.DistanceUndefinedException e) {
        distance = -1;
      }

      out.value = distance;
    }
  }


  @FunctionTemplate(names = {"h3Distance", "h3_distance"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class h3StringDistance implements DrillSimpleFunc {

    @Param
    VarCharHolder startHolder;

    @Param
    VarCharHolder endHolder;

    @Output
    BigIntHolder out;

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

      String start = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(startHolder);
      String end = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(endHolder);
      int distance;
      try {
        distance = h3.h3Distance(start, end);
      } catch (com.uber.h3core.exceptions.DistanceUndefinedException e) {
        distance = -1;
      }

      out.value = distance;
    }
  }
}
