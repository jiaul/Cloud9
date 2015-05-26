package edu.umd.cloud9.integration.webgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import tl.lin.data.array.ArrayListWritable;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.umd.cloud9.integration.IntegrationUtils;
import edu.umd.cloud9.webgraph.DriverUtil;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;

public class ClueWeb09EN01WebgraphIT {
  private static final Random rand = new Random();
  private static final String tmp = "/tmp/tmp-" + ClueWeb09EN01WebgraphIT.class.getSimpleName() +
    rand.nextInt(10000);

  private static final String collectionPath =
    "/shared/collections/ClueWeb09/collection.compressed.block/";
  private static final String docnoMapping =
    "/shared/collections/ClueWeb09/docno-mapping.dat";
  private static final String collectionOutput = tmp + "/webgraph-clueweb09";

  // Galago: part 00000, part 00010
  private ImmutableMap<Integer, String> urlMap = ImmutableMap.of(
    200, "http://160.254.123.37/adr_index_performance_review.jsp",
    600, "http://207.218.246.235/s/spiderman4/",
    10, "http://00perdomain.com/computers/",
    610, "http://207.218.246.235/s/startrek11/news/863_Tyler_Perry_Joins_Star_Trek_11_Cast.html");

  // Galago: part 00000, part 00010
  private ImmutableMap<Integer, ImmutableSet<Integer>> internalLinkMap = ImmutableMap.of(
    200,
    ImmutableSet.of(207,208,209,210,201,202,203,204,205,206),
    600,
    ImmutableSet.of(520,615,616,619,526,480,481,529,533,487,629,601,
                    585,492,591,641,596,646,506,507,602,603,604,605,
                    559,651,467,468),
    10,
    ImmutableSet.of(11,13,6),
    610,
    ImmutableSet.of(520,615,619,480,481,626,486,487,629,600,614,492,533,591,
                    640,641,548,596,646,506,507,651,605,559,467,468));

  private ImmutableMap<Integer, ImmutableSet<Integer>> externalLinkMap = ImmutableMap.of(
    600,
    ImmutableSet.of(31937044));

  @Test
  public void runTests() throws Exception {
    runClueDriver();
    verifyWebGraph();
  }

  private void runClueDriver() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(collectionOutput), true);

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.webgraph.driver.ClueWebDriver.class.getCanonicalName(),
        "-input", collectionPath,
        "-output", collectionOutput,
        "-docno", docnoMapping,
        "-begin", "1",
        "-end", "1",
        "-il",
        "-normalizer", edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer.class.getCanonicalName()};

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  private void verifyWebGraph() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    ArrayListWritable<AnchorText> value = new ArrayListWritable<AnchorText>();

    reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(
        new Path(collectionOutput + "/" + DriverUtil.OUTPUT_WEBGRAPH + "/part-00000")));
    reader.next(key, value); //read key 200
    verifyURLs(200, urlMap, value);
    verifyLinks(200, AnchorTextConstants.Type.INTERNAL_OUT_LINK.val, internalLinkMap, value);

    reader.next(key, value); //skip key 400
    reader.next(key, value); //read key 600
    verifyURLs(600, urlMap, value);
    verifyLinks(600, AnchorTextConstants.Type.INTERNAL_OUT_LINK.val, internalLinkMap, value);
    verifyLinks(600, AnchorTextConstants.Type.EXTERNAL_OUT_LINK.val, externalLinkMap, value);
    reader.close();

    reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(
        new Path(collectionOutput + "/" + DriverUtil.OUTPUT_WEBGRAPH + "/part-00010")));
    reader.next(key, value); //read key 10
    verifyURLs(10, urlMap, value);
    verifyLinks(10, AnchorTextConstants.Type.INTERNAL_OUT_LINK.val, internalLinkMap, value);

    reader.next(key, value); //skip key 210
    reader.next(key, value); //skip key 410
    reader.next(key, value); //read key 610
    verifyURLs(610, urlMap, value);
    verifyLinks(610, AnchorTextConstants.Type.INTERNAL_OUT_LINK.val, internalLinkMap, value);
    reader.close();
  }

  private void verifyURLs(int key, Map<Integer, String> urls,
                          ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if(value.get(i).isURL()) {
        assertEquals(urls.get(key), value.get(i).getText());
        break;
      }
    }
  }

  private void verifyLinks(int key, byte type,
                           Map<Integer, ImmutableSet<Integer>> links,
                           ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if((value.get(i).isInternalOutLink() && type == AnchorTextConstants.Type.INTERNAL_OUT_LINK.val) ||
         (value.get(i).isExternalOutLink() && type == AnchorTextConstants.Type.EXTERNAL_OUT_LINK.val)) {
        int[] targets = value.get(i).getDocuments();
        assertEquals(links.get(key).size(), targets.length);
        for(int j = 0; j < targets.length; j++) {
          assertTrue(links.get(key).contains(targets[j]));
        }
      }
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ClueWeb09EN01WebgraphIT.class);
  }
}
