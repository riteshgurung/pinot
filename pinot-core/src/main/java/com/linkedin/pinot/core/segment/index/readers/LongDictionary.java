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
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import java.io.File;
import java.io.IOException;


/**
 * Nov 14, 2014
 */

public class LongDictionary extends ImmutableDictionaryReader {

  public LongDictionary(File dictFile, ColumnMetadata metadata, ReadMode loadMode) throws IOException {
    super(dictFile, metadata.getCardinality(), Long.SIZE / 8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Long lookup;
    if (rawValue instanceof String) {
      lookup = new Long(Long.parseLong((String) rawValue));
    } else {
      lookup = (Long) rawValue;
    }
    return longIndexOf(lookup.longValue());
  }

  @Override
  public Long get(int dictionaryId) {
    return new Long(getLong(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return getLong(dictionaryId);
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return getLong(dictionaryId);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return new Long(getLong(dictionaryId)).toString();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getLong(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (int) getLong(dictionaryId);
  }

  @Override
  public String toString(int dictionaryId) {
    return new Long(getLong(dictionaryId)).toString();
  }

  private long getLong(int dictionaryId) {
    return dataFileReader.getLong(dictionaryId, 0);
  }

}
