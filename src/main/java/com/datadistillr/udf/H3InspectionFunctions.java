package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;


/**
 * This collection of UDFs follows the pattern here: https://h3geo.org/docs/api/inspection
 */
public class H3InspectionFunctions {

  @FunctionTemplate(names = {"getResolution", "get_resolution"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getResolutionUDF implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Input;

    @Output
    IntHolder result;

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
        result.value = 0;
      } else {
        result.value = h3.h3GetResolution(h3Input.value);
      }
    }
  }

  @FunctionTemplate(names = {"getResolution", "get_resolution"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getResolutionFromStringUDF implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Input;

    @Output
    IntHolder result;

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
      if (h3 != null) {
        String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Input);
        result.value = h3.h3GetResolution(h3InputString);
      }
    }
  }

  @FunctionTemplate(names = {"getBaseCell", "get_base_cell"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getBaseCellUDF implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Input;

    @Output
    IntHolder result;

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
        result.value = 0;
      } else {
        result.value = h3.h3GetBaseCell(h3Input.value);
      }
    }
  }

  @FunctionTemplate(names = {"getBaseCell", "get_base_cell"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getBaseCellFromStringUDF implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Input;

    @Output
    IntHolder result;

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
      if (h3 != null) {
        String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Input);
        result.value = h3.h3GetBaseCell(h3InputString);
      }
    }
  }

  @FunctionTemplate(names = {"stringToH3", "string_to_h3"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class stringToH3 implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Address;

    @Output
    BigIntHolder result;

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
      if (h3 != null) {
        String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Address);
        result.value = h3.stringToH3(h3InputString);
      } else {
        result.value = 0L;
      }
    }
  }


  @FunctionTemplate(names = {"h3ToString", "h3_to_string"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3ToStringUDF implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Address;

    @Output
    VarCharHolder out;

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
      if (h3 != null) {
        String h3AddressString = h3.h3ToString(h3Address.value);
        out.buffer = buffer;
        out.start = 0;
        out.end = h3AddressString.getBytes().length;
        buffer.setBytes(0, h3AddressString.getBytes());
      }
    }
  }

  @FunctionTemplate(names = {"h3IsValid", "h3_is_valid"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3IsValid implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Address;

    @Output
    BitHolder result;

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
        result.value = 0;
      } else {
        result.value = h3.h3IsValid(h3Address.value) ? 1 : 0;
      }
    }
  }

  @FunctionTemplate(names = {"h3IsValid", "h3_is_valid"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3StringIsValid implements DrillSimpleFunc {

    @Param
    VarCharHolder h3AddressString;

    @Output
    BitHolder result;

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
        result.value = 0;
      } else {
        String h3Address = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3AddressString);
        result.value = h3.h3IsValid(h3Address) ? 1 : 0;
      }
    }
  }

  @FunctionTemplate(names = {"h3IsResClassIII", "h3_is_res_class_iii"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3IsResClass3 implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Address;

    @Output
    BitHolder result;

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
        result.value = 0;
      } else {
        result.value = h3.h3IsResClassIII(h3Address.value) ? 1 : 0;
      }
    }
  }

  @FunctionTemplate(names = {"h3IsResClassIII", "h3_is_res_class_iii"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class h3StringIsResClass3 implements DrillSimpleFunc {

    @Param
    VarCharHolder h3AddressString;

    @Output
    BitHolder result;

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
        result.value = 0;
      } else {
        String h3Address = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3AddressString);
        result.value = h3.h3IsResClassIII(h3Address) ? 1 : 0;
      }
    }
  }


}
