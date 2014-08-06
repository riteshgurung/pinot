package com.linkedin.pinot.core.query.utils;

import java.util.Iterator;
import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.operator.DataSource;


public class SimpleIndexSegment implements IndexSegment {

  private Map<String, ColumnarReader> _dataMap;
  private SegmentMetadata _segmentMetadata = new SimpleSegmentMetadata();
  private String _segmentName;
  private String _associatedDir;
  private IndexType _indexType;
  private long _numRecords;

  public SimpleIndexSegment(long numRecords, Map<String, ColumnarReader> dataMap) {
    _indexType = IndexType.simple;
    _dataMap = dataMap;
    _numRecords = numRecords;
    _segmentName = "simpleIndexSegment-" + System.currentTimeMillis();
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public IndexType getIndexType() {
    return _indexType;
  }

  @Override
  public Iterator<Integer> getDocIdIterator(BrokerRequest brokerRequest) {

    return getDocIdIterator();
  }

  @Override
  public String getAssociatedDirectory() {
    return _associatedDir;
  }

  private Iterator<Integer> getDocIdIterator() {
    Iterator<Integer> iterator = new Iterator<Integer>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return (i < _numRecords);
      }

      @Override
      public Integer next() {
        return i++;
      }

      @Override
      public void remove() {

      }
    };
    return iterator;
  }

  @Override
  public ColumnarReader getColumnarReader(String column) {
    return _dataMap.get(column);
  }

  @Override
  public DataSource getDataSource(String columnName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    // TODO Auto-generated method stub
    return null;
  }
}
