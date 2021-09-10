package com.datadistillr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

/**
 * These UDFs mirror the H3 functionality here:  https://h3geo.org/docs/api/indexing.
 */
public class H3IndexingUDFs {

  @FunctionTemplate(names = {"geoToH3", "geo_to_h3"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GeoToH3Function implements DrillSimpleFunc {

    @Param
    Float8Holder latitudeHolder;

    @Param
    Float8Holder longitudeHolder;

    @Param
    IntHolder resolutionHolder;

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
      double latitude = latitudeHolder.value;
      double longitude = longitudeHolder.value;
      int resolution = resolutionHolder.value;
      if (h3 == null) {
        result.value = 0L;
      } else {
        result.value = h3.geoToH3(latitude, longitude, resolution);
      }
    }
  }

  @FunctionTemplate(names = {"geoToH3Address", "geo_to_h3_address"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GeoToH3AddressFunction implements DrillSimpleFunc {

    @Param
    Float8Holder latitudeHolder;

    @Param
    Float8Holder longitudeHolder;

    @Param
    IntHolder resolutionHolder;

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
      double latitude = latitudeHolder.value;
      double longitude = longitudeHolder.value;
      int resolution = resolutionHolder.value;

      if (h3 != null) {
        String result = h3.geoToH3Address(latitude, longitude, resolution);

        out.buffer = buffer;
        out.start = 0;
        out.end = result.getBytes().length;
        buffer.setBytes(0, result.getBytes());
      }
    }
  }

  @FunctionTemplate(names = {"h3ToGeoPoint", "h3_to_geo_point"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GeoToH3GeoPoint implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Input;

    @Output
    VarBinaryHolder out;

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

      com.uber.h3core.util.GeoCoord coord = h3.h3ToGeo(h3Input.value);

      double lon = coord.lng;
      double lat = coord.lat;

      com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(
        new com.esri.core.geometry.Point(lon, lat), com.esri.core.geometry.SpatialReference.create(4326));

      java.nio.ByteBuffer pointBytes = point.asBinary();
      out.buffer = buffer;
      out.start = 0;
      out.end = pointBytes.remaining();
      buffer.setBytes(0, pointBytes);
    }
  }

  @FunctionTemplate(names = {"h3ToGeoPoint", "h3_to_geo_point"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GeoToH3GeoPointFromString implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Input;

    @Output
    VarBinaryHolder out;

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
      String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Input);
      com.uber.h3core.util.GeoCoord coord = h3.h3ToGeo(h3InputString);

      double lon = coord.lng;
      double lat = coord.lat;

      com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(
        new com.esri.core.geometry.Point(lon, lat), com.esri.core.geometry.SpatialReference.create(4326));

      java.nio.ByteBuffer pointBytes = point.asBinary();
      out.buffer = buffer;
      out.start = 0;
      out.end = pointBytes.remaining();
      buffer.setBytes(0, pointBytes);
    }
  }


  @FunctionTemplate(names = {"h3ToGeo", "h3_to_geo"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class GeoToH3GeoMap implements DrillSimpleFunc {

    @Param
    BigIntHolder h3Input;

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

      com.uber.h3core.util.GeoCoord coord = h3.h3ToGeo(h3Input.value);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();
      double lon = coord.lng;
      double lat = coord.lat;

      queryMapWriter.float8("latitude").writeFloat8(lat);
      queryMapWriter.float8("longitude").writeFloat8(lon);
    }
  }


  @FunctionTemplate(names = {"h3ToGeo", "h3_to_geo"},
    scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class GeoToH3GeoMapFromString implements DrillSimpleFunc {

    @Param
    VarCharHolder h3Input;

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

      String h3InputString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(h3Input);

      com.uber.h3core.util.GeoCoord coord = h3.h3ToGeo(h3InputString);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();
      double lon = coord.lng;
      double lat = coord.lat;

      queryMapWriter.float8("latitude").writeFloat8(lat);
      queryMapWriter.float8("longitude").writeFloat8(lon);
    }
  }
}
