/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.linkedin.pinot.common.request;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked" })
/**
 * AUTO GENERATED: DO NOT EDIT
 * Query source
 * 
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-5-2")
public class QuerySource implements org.apache.thrift.TBase<QuerySource, QuerySource._Fields>, java.io.Serializable, Cloneable, Comparable<QuerySource> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("QuerySource");

  private static final org.apache.thrift.protocol.TField RESOURCE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("resourceName", org.apache.thrift.protocol.TType.STRING,
      (short) 1);
  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new QuerySourceStandardSchemeFactory());
    schemes.put(TupleScheme.class, new QuerySourceTupleSchemeFactory());
  }

  private String resourceName; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RESOURCE_NAME((short) 1, "resourceName"),
    TABLE_NAME((short) 2, "tableName");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: // RESOURCE_NAME
          return RESOURCE_NAME;
        case 2: // TABLE_NAME
          return TABLE_NAME;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null)
        throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = { _Fields.RESOURCE_NAME, _Fields.TABLE_NAME };
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RESOURCE_NAME, new org.apache.thrift.meta_data.FieldMetaData("resourceName", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLE_NAME, new org.apache.thrift.meta_data.FieldMetaData("tableName", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(QuerySource.class, metaDataMap);
  }

  public QuerySource() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public QuerySource(QuerySource other) {
    if (other.isSetResourceName()) {
      this.resourceName = other.resourceName;
    }
  }

  public QuerySource deepCopy() {
    return new QuerySource(this);
  }

  @Override
  public void clear() {
    this.resourceName = null;
  }

  public String getResourceName() {
    return this.resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public void unsetResourceName() {
    this.resourceName = null;
  }

  /** Returns true if field resourceName is set (has been assigned a value) and false otherwise */
  public boolean isSetResourceName() {
    return this.resourceName != null;
  }

  public void setResourceNameIsSet(boolean value) {
    if (!value) {
      this.resourceName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case RESOURCE_NAME:
        if (value == null) {
          unsetResourceName();
        } else {
          setResourceName((String) value);
        }
        break;
    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
      case RESOURCE_NAME:
        return getResourceName();
    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
      case RESOURCE_NAME:
        return isSetResourceName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof QuerySource)
      return this.equals((QuerySource) that);
    return false;
  }

  public boolean equals(QuerySource that) {
    if (that == null)
      return false;

    boolean this_present_resourceName = true && this.isSetResourceName();
    boolean that_present_resourceName = true && that.isSetResourceName();
    if (this_present_resourceName || that_present_resourceName) {
      if (!(this_present_resourceName && that_present_resourceName))
        return false;
      if (!this.resourceName.equals(that.resourceName))
        return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_resourceName = true && (isSetResourceName());
    list.add(present_resourceName);
    if (present_resourceName)
      list.add(resourceName);
    return list.hashCode();
  }

  @Override
  public int compareTo(QuerySource other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetResourceName()).compareTo(other.isSetResourceName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetResourceName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.resourceName, other.resourceName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("QuerySource(");
    boolean first = true;

    if (isSetResourceName()) {
      sb.append("resourceName:");
      if (this.resourceName == null) {
        sb.append("null");
      } else {
        sb.append(this.resourceName);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class QuerySourceStandardSchemeFactory implements SchemeFactory {
    public QuerySourceStandardScheme getScheme() {
      return new QuerySourceStandardScheme();
    }
  }

  private static class QuerySourceStandardScheme extends StandardScheme<QuerySource> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, QuerySource struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // RESOURCE_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.resourceName = iprot.readString();
              struct.setResourceNameIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, QuerySource struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.resourceName != null) {
        if (struct.isSetResourceName()) {
          oprot.writeFieldBegin(RESOURCE_NAME_FIELD_DESC);
          oprot.writeString(struct.resourceName);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class QuerySourceTupleSchemeFactory implements SchemeFactory {
    public QuerySourceTupleScheme getScheme() {
      return new QuerySourceTupleScheme();
    }
  }

  private static class QuerySourceTupleScheme extends TupleScheme<QuerySource> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, QuerySource struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetResourceName()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetResourceName()) {
        oprot.writeString(struct.resourceName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, QuerySource struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.resourceName = iprot.readString();
        struct.setResourceNameIsSet(true);
      }
    }
  }

}
