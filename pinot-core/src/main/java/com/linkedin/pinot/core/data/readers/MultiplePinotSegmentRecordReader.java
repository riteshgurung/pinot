/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


/**
 * Record reader for multiple pinot segments.
 */
public class MultiplePinotSegmentRecordReader implements RecordReader {
  private List<PinotSegmentRecordReader> _pinotSegmentRecordReaders;
  private PriorityQueue<GenericRowWithReader> _queue;
  private Schema _schema;
  private String _sortedColumn;
  private int _currentReaderId;

  /**
   * Read records using the passed in schema from multiple pinot segments.
   * <p>Passed in schema must be a subset of the segment schema.
   */
  public MultiplePinotSegmentRecordReader(List<File> indexDirs, Schema schema) throws Exception {
    _pinotSegmentRecordReaders = new ArrayList<>(indexDirs.size());
    for (File file : indexDirs) {
      _pinotSegmentRecordReaders.add(new PinotSegmentRecordReader(file, schema));
    }
    _schema = schema;
  }

  /**
   * Read records using the passed in schema and in the order of sorted column from multiple pinot segments.
   */
  public MultiplePinotSegmentRecordReader(List<File> indexDirs, Schema schema, String sortedColumn) throws Exception {
    _pinotSegmentRecordReaders = new ArrayList<>(indexDirs.size());
    for (File file : indexDirs) {
      _pinotSegmentRecordReaders.add(new PinotSegmentRecordReader(file, schema, sortedColumn));
    }
    _schema = schema;
    _sortedColumn = sortedColumn;

    // Initialize the priority queue if the sorted column is specified.
    if (_sortedColumn != null && getSchema().getFieldSpecFor(sortedColumn) != null) {
      _queue = new PriorityQueue<>(_pinotSegmentRecordReaders.size());
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          _queue.add(new GenericRowWithReader(recordReader.next(), recordReader, _sortedColumn));
        }
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (_sortedColumn != null) {
      return _queue.size() > 0;
    } else {
      boolean hasNext = false;
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          hasNext = true;
        }
      }
      return hasNext;
    }
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    if (_sortedColumn != null) {
      GenericRowWithReader genericRowComparable = _queue.poll();
      GenericRow currentRow = genericRowComparable.getRow();

      // Fill reuse with the information from the currentRow
      reuse.clear();
      for (Map.Entry<String, Object> entry : currentRow.getEntrySet()) {
        reuse.putField(entry.getKey(), entry.getValue());
      }

      // If the record reader has more rows left, put back the next minimum value to the queue
      PinotSegmentRecordReader recordReader = genericRowComparable.getRecordReader();
      if (recordReader.hasNext()) {
        genericRowComparable.setRow(recordReader.next(currentRow));
        genericRowComparable.setRecordReader(recordReader);
        _queue.add(genericRowComparable);
      }
      return reuse;
    } else {
      // If there is no sorted column specified, simply concatenate the segments
      PinotSegmentRecordReader currentReader = _pinotSegmentRecordReaders.get(_currentReaderId);
      if (!currentReader.hasNext()) {
        _currentReaderId++;
        if (_currentReaderId >= _pinotSegmentRecordReaders.size()) {
          throw new RuntimeException("next is called after reading all data");
        }
        currentReader = _pinotSegmentRecordReaders.get(_currentReaderId);
      }
      return currentReader.next(reuse);
    }
  }

  @Override
  public void rewind() throws IOException {
    for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
      recordReader.rewind();
    }
    // If sorted column is specified, we need to re-initialize the priority queue
    if (_queue != null) {
      _queue.clear();
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          _queue.add(new GenericRowWithReader(recordReader.next(), recordReader, _sortedColumn));
        }
      }
    } else {
      _currentReaderId = 0;
    }
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() throws IOException {
    for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
      recordReader.close();
    }
  }

  /**
   * Wrapper for generic row and record reader along with sorted column.
   * Comparison of this object is based on the value of sorted column.
   */
  class GenericRowWithReader implements Comparable<GenericRowWithReader> {
    private GenericRow _row;
    private PinotSegmentRecordReader _recordReader;
    private String _sortedColumn;

    public GenericRowWithReader(GenericRow row, PinotSegmentRecordReader recordReader, String sortedColumn) {
      _row = row;
      _recordReader = recordReader;
      _sortedColumn = sortedColumn;
    }

    @Override
    public int compareTo(GenericRowWithReader o) {
      Object otherVal = o.getRow().getValue(_sortedColumn);
      Object thisVal = _row.getValue(_sortedColumn);
      if (thisVal instanceof String) {
        return ((String) thisVal).compareTo((String) otherVal);
      } else if (thisVal instanceof Integer) {
        return ((Integer) thisVal).compareTo((Integer) otherVal);
      } else if (thisVal instanceof Float) {
        return ((Float) thisVal).compareTo((Float) otherVal);
      } else if (thisVal instanceof Long) {
        return ((Long) thisVal).compareTo((Long) otherVal);
      } else if (thisVal instanceof Double) {
        return ((Double) thisVal).compareTo((Double) otherVal);
      }
      throw new IllegalStateException("Unsupported column value type");
    }

    public GenericRow getRow() {
      return _row;
    }

    public void setRow(GenericRow row) {
      _row = row;
    }

    public PinotSegmentRecordReader getRecordReader() {
      return _recordReader;
    }

    public void setRecordReader(PinotSegmentRecordReader recordReader) {
      _recordReader = recordReader;
    }
  }
}
