/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.reader.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.MmapUtils;


/**
 *
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br>
 * 1. &lt;TYPE&gt; getType(int row, int col) this is used for single value column <br>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br>
 *
 *
 */
public class FixedByteSingleValueMultiColReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleValueMultiColReader.class);

  RandomAccessFile file;
  private final int rows;
  private final int cols;
  private final int[] colOffSets;
  private int rowSize;
  private ByteBuffer byteBuffer;
  private final int[] columnSizes;
  private final boolean isMMap;

  private File dataFile;

  /**
   *
   * @param file
   * @param rows
   * @param cols
   * @param columnSizes
   * @return
   * @throws IOException
   */
  public static FixedByteSingleValueMultiColReader forHeap(File file,
      int rows, int cols, int[] columnSizes) throws IOException {
    return new FixedByteSingleValueMultiColReader(file, rows, cols,
        columnSizes, false);
  }

  /**
   *
   * @param file
   * @param rows
   * @param cols
   * @param columnSizes
   * @return
   * @throws IOException
   */
  public static FixedByteSingleValueMultiColReader forMmap(File file,
      int rows, int cols, int[] columnSizes) throws IOException {
    return new FixedByteSingleValueMultiColReader(file, rows, cols,
        columnSizes, true);
  }

  /**
   *
   * @param dataFile
   * @param rows
   * @param cols
   * @param columnSizes
   *            in bytes
   * @throws IOException
   */
  public FixedByteSingleValueMultiColReader(File dataFile, int rows,
      int cols, int[] columnSizes, boolean isMmap) throws IOException {
    this.dataFile = dataFile;
    this.rows = rows;
    this.cols = cols;
    this.columnSizes = columnSizes;
    this.isMMap = isMmap;
    colOffSets = new int[columnSizes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizes[i];
    }
    file = new RandomAccessFile(dataFile, "rw");
    final long totalSize = rowSize * rows;
    if (isMmap) {
      byteBuffer = file.getChannel()
          .map(FileChannel.MapMode.READ_ONLY, 0, totalSize)
          .order(ByteOrder.BIG_ENDIAN);
    } else {
      byteBuffer = MmapUtils.allocateDirectByteBuffer((int) totalSize, dataFile,
          this.getClass().getSimpleName() + " byteBuffer");
      file.getChannel().read(byteBuffer);
      file.close();
    }
  }

  public FixedByteSingleValueMultiColReader(ByteBuffer buffer, int rows,
      int cols, int[] columnSizes) throws IOException {
    this.rows = rows;
    this.cols = cols;
    this.columnSizes = columnSizes;
    this.isMMap = false;
    colOffSets = new int[columnSizes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizes[i];
    }
    byteBuffer = buffer;
  }

  public FixedByteSingleValueMultiColReader(String fileName, int rows,
      int cols, int[] columnSizes) throws IOException {
    this(new File(fileName), rows, cols, columnSizes, true);
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    if (row >= rows || col >= cols) {
      final String message = String.format(
          "Input (%d,%d) is not with in expected range (%d,%d for column:%s)", row,
          col, rows, cols,dataFile.getAbsolutePath());
      throw new IndexOutOfBoundsException(message);
    }
    final int offset = row * rowSize + colOffSets[col];
    return offset;
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public char getChar(int row, int col) {
    final int offset = computeOffset(row, col);
    return byteBuffer.getChar(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    final int offset = computeOffset(row, col);
    return byteBuffer.getShort(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public int getInt(int row, int col) {
    assert getColumnSizes()[col] == 4;
    final int offset = computeOffset(row, col);
    return byteBuffer.getInt(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public long getLong(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return byteBuffer.getLong(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public float getFloat(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return byteBuffer.getFloat(offset);
  }

  /**
   * Reads the double at row,col
   *
   * @param row
   * @param col
   * @return
   */
  public double getDouble(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return byteBuffer.getDouble(offset);
  }

  /**
   * Returns the string value, NOTE: It expects all String values in the file
   * to be of same length
   *
   * @param row
   * @param col
   * @return
   */
  public String getString(int row, int col) {
    return new String(getBytes(row, col), Charset.forName("UTF-8"));
  }

  /**
   * Generic method to read the raw bytes
   *
   * @param row
   * @param col
   * @return
   */
  public byte[] getBytes(int row, int col) {
    final int length = getColumnSizes()[col];
    final byte[] dst = new byte[length];
    final int offset = computeOffset(row, col);
    // [PINOT-2381] duplicate buffer for thread-safety before setting position
    // duplicate() and absolute get() methods themselves are not guaranteed
    // to be thread-safe but those are "fairly"/practically safe.
    // Thread-locals are one way to guarantee thread-safety
    // of bytebuffer but that can be costly.
    // TODO/atumbde: we are investigating the performance impact of
    // thread-locals before introducing those. This fix is temporary
    // fix to get correctness in query comparisons for migration
    ByteBuffer bufferCopy = byteBuffer.duplicate();
    bufferCopy.position(offset);
    bufferCopy.get(dst, 0, length);
    return dst;
  }

  public int getNumberOfRows() {
    return rows;
  }

  public int getNumberOfCols() {
    return rows;
  }

  public int[] getColumnSizes() {
    return columnSizes;
  }

  @Override
  public void close() throws IOException {
    MmapUtils.unloadByteBuffer(byteBuffer);
    byteBuffer = null;

    if (isMMap) {
      file.close();
    }
  }

  public boolean open() {
    return false;
  }

  public void readIntValues(int[] rows, int col, int startPos, int limit, int[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getInt(rows[iter], col);
    }
  }

  public void readLongValues(int[] rows, int col, int startPos, int limit, long[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getLong(rows[iter], col);
    }
  }
  public void readFloatValues(int[] rows, int col, int startPos, int limit, float[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getFloat(rows[iter], col);
    }
  }

  public void readDoubleValues(int[] rows, int col, int startPos, int limit, double[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getDouble(rows[iter], col);
    }
  }
  public void readStringValues(int[] rows, int col, int startPos, int limit, String[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getString(rows[iter], col);
    }
  }

}
