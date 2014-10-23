package com.linkedin.pinot.query.aggregation;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestAggregationGroupByWithDictionaryOperator {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestAggregationGroupByWithDictionaryOperator.class.toString());
  private static File INDEXES_DIR = new File(TestAggregationGroupByWithDictionaryOperator.class.toString() + "_LIST");

  public static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  public static AggregationInfo _paramsInfo;
  public static List<AggregationInfo> _aggregationInfos;
  public static int _numAggregations = 6;

  public Map<String, Dictionary<?>> _dictionaryMap;
  public Map<String, ColumnMetadata> _medataMap;
  public static GroupBy _groupBy;

  @BeforeClass
  public void setup() throws Exception {
    setupSegment();
    setupQuery();

    _indexSegment = ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    _dictionaryMap = ((ColumnarSegment) _indexSegment).getDictionaryMap();
    _medataMap = ((ColumnarSegment) _indexSegment).getColumnMetadataMap();
    _indexSegmentList = new ArrayList<IndexSegment>();

  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      SegmentGeneratorConfiguration config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir,
              "daysSinceEpoch", SegmentTimeUnit.days, "test", "testTable");

      ColumnarSegmentCreator creator =
          (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
      creator.init(config);
      creator.buildSegment();

      System.out.println("built at : " + segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(segmentDir, ReadMode.heap));
    }
  }

  public void setupSegment() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  public void setupQuery() {
    _aggregationInfos = getAggregationsInfo();
    List<String> groupbyColumns = new ArrayList<String>();
    groupbyColumns.add("dim_memberGender");
    _groupBy = new GroupBy();
    _groupBy.setColumns(groupbyColumns);
    _groupBy.setTopN(10);
  }

  @Test
  public void testAggregationGroupBys() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    Map<String, DataSource> dataSourceMap = getDataSourceMap();
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      MAggregationFunctionGroupByWithDictionaryOperator aggregationFunctionGroupByOperator =
          new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator));
      aggregationFunctionGroupByOperatorList.add(aggregationFunctionGroupByOperator);
    }

    MAggregationGroupByOperator aggregationGroupByOperator =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator,
            aggregationFunctionGroupByOperatorList);

    System.out.println("running query: ");
    IntermediateResultsBlock block = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();

    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationGroupByOperatorResult().get(i));
    }
  }

  @Test
  public void testAggregationGroupBysWithCombine() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    Map<String, DataSource> dataSourceMap = getDataSourceMap();
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      MAggregationFunctionGroupByWithDictionaryOperator aggregationFunctionGroupByOperator =
          new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator));
      aggregationFunctionGroupByOperatorList.add(aggregationFunctionGroupByOperator);
    }

    MAggregationGroupByOperator aggregationGroupByOperator =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator,
            aggregationFunctionGroupByOperatorList);

    IntermediateResultsBlock block = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();

    System.out.println("Result 1: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationGroupByOperatorResult().get(i));
    }

    ////////////////////////////////////////////////////////////////////////
    List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList1 =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator1 = new BDocIdSetOperator(null, _indexSegment, 5000);
    Map<String, DataSource> dataSourceMap1 = getDataSourceMap();
    MProjectionOperator projectionOperator1 = new MProjectionOperator(dataSourceMap1, docIdSetOperator1);

    for (int i = 0; i < _numAggregations; ++i) {
      MAggregationFunctionGroupByWithDictionaryOperator aggregationFunctionGroupByOperator1 =
          new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator1));
      aggregationFunctionGroupByOperatorList1.add(aggregationFunctionGroupByOperator1);
    }

    MAggregationGroupByOperator aggregationGroupByOperator1 =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator1,
            aggregationFunctionGroupByOperatorList1);
    ////////////////////////////////////////////////////////////

    IntermediateResultsBlock block1 = (IntermediateResultsBlock) aggregationGroupByOperator1.nextBlock();

    System.out.println("Result 2: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block1.getAggregationGroupByOperatorResult().get(i));
    }

    CombineService.mergeTwoBlocks(getAggregationGroupByNoFilterBrokerRequest(), block, block1);

    System.out.println("Combined Result : ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationGroupByOperatorResult().get(i));
    }
  }

  @Test
  public void testAggregationGroupBysWithDataTableEncodeAndDecode() throws Exception {
    List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    Map<String, DataSource> dataSourceMap = getDataSourceMap();
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      MAggregationFunctionGroupByWithDictionaryOperator aggregationFunctionGroupByOperator =
          new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator));
      aggregationFunctionGroupByOperatorList.add(aggregationFunctionGroupByOperator);
    }

    MAggregationGroupByOperator aggregationGroupByOperator =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator,
            aggregationFunctionGroupByOperatorList);

    IntermediateResultsBlock block = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();

    System.out.println("Result 1: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationGroupByOperatorResult().get(i));
    }
    ////////////////////////////////////////////////////////////////////////
    List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList1 =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator1 = new BDocIdSetOperator(null, _indexSegment, 5000);
    Map<String, DataSource> dataSourceMap1 = getDataSourceMap();
    MProjectionOperator projectionOperator1 = new MProjectionOperator(dataSourceMap1, docIdSetOperator1);

    for (int i = 0; i < _numAggregations; ++i) {
      MAggregationFunctionGroupByWithDictionaryOperator aggregationFunctionGroupByOperator1 =
          new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator1));
      aggregationFunctionGroupByOperatorList1.add(aggregationFunctionGroupByOperator1);
    }

    MAggregationGroupByOperator aggregationGroupByOperator1 =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator1,
            aggregationFunctionGroupByOperatorList1);
    ////////////////////////////////////////////////////////////

    IntermediateResultsBlock block1 = (IntermediateResultsBlock) aggregationGroupByOperator1.nextBlock();

    System.out.println("Result 2: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block1.getAggregationGroupByOperatorResult().get(i));
    }
    CombineService.mergeTwoBlocks(getAggregationGroupByNoFilterBrokerRequest(), block, block1);

    System.out.println("Combined Result : ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationGroupByOperatorResult().get(i));
    }
    DataTable dataTable = block.getAggregationGroupByResultDataTable();

    List<Map<String, Serializable>> results =
        AggregationGroupByOperatorService.transformDataTableToGroupByResult(dataTable);
    System.out.println("Decode AggregationResult from DataTable: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(results.get(i));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByOperatorNoFilter() throws Exception {
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // UAggregationGroupByOperator operator = (UAggregationGroupByOperator) rootPlanNode.run();
    MAggregationGroupByOperator operator = (MAggregationGroupByOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    List<Map<String, Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByOperatorResult();

    //    System.out.println("********************************");
    //    for (int i = 0; i < combinedGroupByResult.size(); ++i) {
    //      Map<String, Serializable> groupByResult = combinedGroupByResult.get(i);
    //      System.out.println(groupByResult);
    //    }
    //    System.out.println("********************************");
    AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(_aggregationInfos, brokerRequest.getGroupBy());

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getAggregationGroupByResultDataTable());
    List<Map<String, Serializable>> reducedResults =
        aggregationGroupByOperatorService.reduceGroupByOperators(instanceResponseMap);
    //    System.out.println("********************************");
    //    for (int i = 0; i < reducedResults.size(); ++i) {
    //      Map<String, Serializable> groupByResult = reducedResults.get(i);
    //      System.out.println(groupByResult);
    //    }
    //    System.out.println("********************************");
    List<JSONObject> jsonResult = aggregationGroupByOperatorService.renderGroupByOperators(reducedResults);
    System.out.println(jsonResult);
    //    System.out.println("********************************");
  }

  @Test
  public void testInterSegmentAggregationGroupByPlanMakerAndRun() throws Exception {
    int numSegments = 20;
    setupSegmentList(numSegments);
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println(instanceResponse);
    System.out.println("timeUsedMs : " + instanceResponse.getMetadata().get("timeUsedMs"));

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getAggregationResults()));
    System.out.println("Time used : " + brokerResponse.getTimeUsedMs());

    assertBrokerResponse(numSegments, brokerResponse);
  }

  private void assertBrokerResponse(int numSegments, BrokerResponse brokerResponse) throws JSONException {
    Assert.assertEquals(10001 * numSegments, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(_numAggregations, brokerResponse.getAggregationResults().size());
    for (int i = 0; i < _numAggregations; ++i) {
      Assert.assertEquals("[\"dim_memberGender\",\"dim_memberFunction\"]", brokerResponse.getAggregationResults()
          .get(i).getJSONArray("groupByColumns").toString());
      Assert.assertEquals(15, brokerResponse.getAggregationResults().get(i).getJSONArray("groupByResult").length());
    }

    // Assertion on Count
    Assert.assertEquals("count_star", brokerResponse.getAggregationResults().get(0).getString("function").toString());
    Assert.assertEquals("sum_met_impressionCount", brokerResponse.getAggregationResults().get(1).getString("function")
        .toString());
    Assert.assertEquals("max_met_impressionCount", brokerResponse.getAggregationResults().get(2).getString("function")
        .toString());
    Assert.assertEquals("min_met_impressionCount", brokerResponse.getAggregationResults().get(3).getString("function")
        .toString());
    Assert.assertEquals("avg_met_impressionCount", brokerResponse.getAggregationResults().get(4).getString("function")
        .toString());
    Assert.assertEquals("distinctCount_dim_memberIndustry",
        brokerResponse.getAggregationResults().get(5).getString("function").toString());

    // Assertion on Aggregation Results
    List<double[]> aggregationResult = getAggregationResult(numSegments);
    List<String[]> groupByResult = getGroupResult();
    for (int j = 0; j < _numAggregations; ++j) {
      double[] aggResult = aggregationResult.get(j);
      String[] groupResult = groupByResult.get(j);
      for (int i = 0; i < 15; ++i) {
        Assert.assertEquals(aggResult[i], brokerResponse.getAggregationResults().get(j).getJSONArray("groupByResult")
            .getJSONObject(i).getDouble("value"));
        Assert.assertEquals(groupResult[i], brokerResponse.getAggregationResults().get(j).getJSONArray("groupByResult")
            .getJSONObject(i).getString("group"));
      }
    }

  }

  private static List<double[]> getAggregationResult(int numSegments) {
    List<double[]> aggregationResultList = new ArrayList<double[]>();
    aggregationResultList.add(getCountResult(numSegments));
    aggregationResultList.add(getSumResult(numSegments));
    aggregationResultList.add(getMaxResult());
    aggregationResultList.add(getMinResult());
    aggregationResultList.add(getAvgResult());
    aggregationResultList.add(getDistinctCountResult());
    return aggregationResultList;
  }

  private static List<String[]> getGroupResult() {
    List<String[]> groupResults = new ArrayList<String[]>();
    groupResults.add(getCountGroupResult());
    groupResults.add(getSumGroupResult());
    groupResults.add(getMaxGroupResult());
    groupResults.add(getMinGroupResult());
    groupResults.add(getAvgGroupResult());
    groupResults.add(getDistinctCountGroupResult());
    return groupResults;
  }

  private static double[] getCountResult(int numSegments) {
    return new double[] { 1450 * numSegments, 620 * numSegments, 517 * numSegments, 422 * numSegments, 365 * numSegments, 340 * numSegments, 321 * numSegments, 296 * numSegments, 286 * numSegments, 273 * numSegments, 271 * numSegments, 268 * numSegments, 234 * numSegments, 210 * numSegments, 208 * numSegments };
  }

  private static String[] getCountGroupResult() {
    return new String[] { "[\"m\",\"\"]", "[\"f\",\"\"]", "[\"m\",\"eng\"]", "[\"m\",\"ent\"]", "[\"m\",\"sale\"]", "[\"m\",\"it\"]", "[\"m\",\"ops\"]", "[\"m\",\"acad\"]", "[\"m\",\"supp\"]", "[\"m\",\"cre\"]", "[\"m\",\"pr\"]", "[\"m\",\"finc\"]", "[\"m\",\"mktg\"]", "[\"m\",\"cnsl\"]", "[\"m\",\"ppm\"]" };
  }

  private static double[] getSumResult(int numSegments) {
    return new double[] { 3848 * numSegments, 1651 * numSegments, 1161 * numSegments, 1057 * numSegments, 869 * numSegments, 842 * numSegments, 765 * numSegments, 737 * numSegments, 691 * numSegments, 687 * numSegments, 680 * numSegments, 645 * numSegments, 610 * numSegments, 567 * numSegments, 516 * numSegments };
  }

  private static String[] getSumGroupResult() {
    return new String[] { "[\"m\",\"\"]", "[\"f\",\"\"]", "[\"m\",\"eng\"]", "[\"m\",\"ent\"]", "[\"m\",\"sale\"]", "[\"m\",\"it\"]", "[\"m\",\"ops\"]", "[\"m\",\"cre\"]", "[\"m\",\"acad\"]", "[\"m\",\"supp\"]", "[\"m\",\"pr\"]", "[\"m\",\"finc\"]", "[\"m\",\"mktg\"]", "[\"m\",\"ppm\"]", "[\"m\",\"cnsl\"]" };
  }

  private static double[] getMaxResult() {
    return new double[] { 53, 22, 18, 16, 16, 15, 14, 11, 11, 11, 11, 11, 11, 10, 10 };
  }

  private static String[] getMaxGroupResult() {
    return new String[] { "[\"m\",\"\"]", "[\"m\",\"pr\"]", "[\"m\",\"edu\"]", "[\"m\",\"bd\"]", "[\"f\",\"\"]", "[\"m\",\"ent\"]", "[\"f\",\"sale\"]", "[\"f\",\"css\"]", "[\"m\",\"supp\"]", "[\"u\",\"\"]", "[\"f\",\"eng\"]", "[\"m\",\"ppm\"]", "[\"m\",\"cre\"]", "[\"m\",\"it\"]", "[\"m\",\"mps\"]" };
  }

  private static double[] getMinResult() {
    return new double[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
  }

  private static String[] getMinGroupResult() {
    return new String[] { "[\"m\",\"bd\"]", "[\"u\",\"hr\"]", "[\"m\",\"prod\"]", "[\"f\",\"prod\"]", "[\"m\",\"it\"]", "[\"f\",\"supp\"]", "[\"m\",\"acad\"]", "[\"m\",\"mps\"]", "[\"f\",\"bd\"]", "[\"f\",\"cnsl\"]", "[\"f\",\"mps\"]", "[\"u\",\"pr\"]", "[\"m\",\"lgl\"]", "[\"f\",\"pr\"]", "[\"m\",\"cnsl\"]" };
  }

  private static double[] getAvgResult() {
    return new double[] { 4, 3, 2.95, 2.93333, 2.87037, 2.80000, 2.75000, 2.72596, 2.71429, 2.69963, 2.68750, 2.68644, 2.66667, 2.66667, 2.66290 };
  }

  private static String[] getAvgGroupResult() {
    return new String[] { "[\"u\",\"buy\"]", "[\"u\",\"re\"]", "[\"u\",\"pr\"]", "[\"u\",\"edu\"]", "[\"f\",\"acct\"]", "[\"u\",\"ppm\"]", "[\"u\",\"hr\"]", "[\"m\",\"ppm\"]", "[\"f\",\"mps\"]", "[\"m\",\"cre\"]", "[\"u\",\"admn\"]", "[\"m\",\"acct\"]", "[\"m\",\"buy\"]", "[\"u\",\"bd\"]", "[\"f\",\"\"]" };
  }

  private static double[] getDistinctCountResult() {
    return new double[] { 129, 110, 101, 99, 84, 81, 77, 76, 75, 74, 71, 67, 67, 62, 57 };
  }

  private static String[] getDistinctCountGroupResult() {
    return new String[] { "[\"m\",\"\"]", "[\"f\",\"\"]", "[\"m\",\"ops\"]", "[\"m\",\"ent\"]", "[\"m\",\"sale\"]", "[\"m\",\"it\"]", "[\"m\",\"supp\"]", "[\"m\",\"eng\"]", "[\"m\",\"mktg\"]", "[\"m\",\"acad\"]", "[\"m\",\"ppm\"]", "[\"u\",\"\"]", "[\"f\",\"mktg\"]", "[\"f\",\"admn\"]", "[\"m\",\"pr\"]" };
  }

  private static BrokerRequest getAggregationGroupByNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = getAggregationsInfo();
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    return brokerRequest;
  }

  private static List<AggregationInfo> getAggregationsInfo() {
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountAggregationInfo("dim_memberIndustry"));
    return aggregationsInfo;
  }

  private static Map<String, DataSource> getDataSourceMap() {
    Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
    dataSourceMap.put("dim_memberGender", _indexSegment.getDataSource("dim_memberGender"));
    dataSourceMap.put("dim_memberIndustry", _indexSegment.getDataSource("dim_memberIndustry"));
    dataSourceMap.put("met_impressionCount", _indexSegment.getDataSource("met_impressionCount"));
    return dataSourceMap;
  }

  private static AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "*");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getAvgAggregationInfo() {
    String type = "avg";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getDistinctCountAggregationInfo(String dim) {
    String type = "distinctCount";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", dim);

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static GroupBy getGroupBy() {
    GroupBy groupBy = new GroupBy();
    List<String> columns = new ArrayList<String>();
    columns.add("dim_memberGender");
    columns.add("dim_memberFunction");
    groupBy.setColumns(columns);
    groupBy.setTopN(15);
    return groupBy;
  }
}
