package edu.umd.cloud9.integration.collection.trec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.CountTrecDocuments;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocnoMappingBuilder;
import edu.umd.cloud9.collection.trec.TrecForwardIndex;
import edu.umd.cloud9.collection.trec.TrecForwardIndexBuilder;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() +
      "-" + random.nextInt(10000);

  private static final String mappingFile = tmpPrefix + "-mapping.dat";

  @Test
  public void testDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    TrecDocnoMappingBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + collectionPath,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + mappingFile });

    TrecDocnoMapping mapping = new TrecDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    assertEquals("FBIS3-1", mapping.getDocid(1));
    assertEquals("LA061490-0139", mapping.getDocid(400000));

    assertEquals(1, mapping.getDocno("FBIS3-1"));
    assertEquals(400000, mapping.getDocno("LA061490-0139"));
  }

  @Test
  public void testDemoCountDocs() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String output = tmpPrefix + "-cnt";
    CountTrecDocuments.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-collection=" + collectionPath,
        "-output=" + output,
        "-docnoMapping=" + mappingFile });
  }

  @Test
  public void testForwardIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String index = tmpPrefix + "-findex.dat";
    TrecForwardIndexBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-collection=" + collectionPath,
        "-index=" + index,
        "-docnoMapping=" + mappingFile });

    TrecForwardIndex findex = new TrecForwardIndex();
    findex.loadIndex(new Path(index), new Path(mappingFile), fs);

    assertTrue(findex.getDocument(1).getContent().contains("Newspapers in the Former Yugoslav Republic"));
    assertTrue(findex.getDocument("FBIS3-1").getContent().contains("Newspapers in the Former Yugoslav Republic"));
    assertEquals(1, findex.getFirstDocno());
    assertEquals(472525, findex.getLastDocno());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
