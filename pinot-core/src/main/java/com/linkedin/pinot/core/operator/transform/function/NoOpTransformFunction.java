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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The <code>NoOpTransformFunction</code> class is a special transform function which is a wrapper on top of a single
 * column, and directly return the column value without any transformation.
 */
public class NoOpTransformFunction implements TransformFunction {
  private String _columnName;
  private DataSource _dataSource;

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(@Nonnull TransformExpressionTree expression, @Nonnull Map<String, DataSource> dataSourceMap) {
    _columnName = expression.getValue();
    _dataSource = dataSourceMap.get(_columnName);
  }

  @Override
  public DataSourceMetadata getResultMetadata() {
    return _dataSource.getDataSourceMetadata();
  }

  @Override
  public Dictionary getDictionary() {
    return _dataSource.getDictionary();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsSV();
  }

  @Override
  public int[][] transformToDictIdsMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsMV();
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesSV();
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesSV();
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesSV();
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesSV();
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesSV();
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesMV();
  }

  @Override
  public long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesMV();
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesMV();
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesMV();
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesMV();
  }
}