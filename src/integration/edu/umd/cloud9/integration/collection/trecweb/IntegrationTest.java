package edu.umd.cloud9.integration.collection.trecweb;

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
import edu.umd.cloud9.collection.trecweb.Gov2DocnoMapping;
import edu.umd.cloud9.collection.trecweb.RepackTrecWebCollection;
import edu.umd.cloud9.collection.trecweb.TrecWebDocnoMappingBuilder;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat;
import edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path wt10gPath = new Path("/shared/collections/wt10g/collection.raw");
  private static final Path gov2Path = new Path("/shared/collections/gov2/collection.raw");

  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() +
      "-" + random.nextInt(10000);

  // wt10g, repacked.
  @Test
  public void testWt10gDocnoMappingRepacked() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(wt10gPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String repackedCollection = tmpPrefix + "-wt10g-repacked";
    String wt10gMappingFile = tmpPrefix + "-wt10g-mapping-repacked.dat";
    RepackTrecWebCollection.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-collection=" + wt10gPath, "-output=" + repackedCollection, "-compressionType=block" });

    TrecWebDocnoMappingBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + repackedCollection,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + wt10gMappingFile });

    Wt10gDocnoMapping mapping = new Wt10gDocnoMapping();
    mapping.loadMapping(new Path(wt10gMappingFile), fs);

    assertEquals("WTX001-B01-1", mapping.getDocid(1));
    assertEquals("WTX062-B34-37", mapping.getDocid(1000000));

    assertEquals(1, mapping.getDocno("WTX001-B01-1"));
    assertEquals(1000000, mapping.getDocno("WTX062-B34-37"));
  }

  // gov2, repacked.
  @Test
  public void testGov2DocnoMappingRepacked() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(gov2Path));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String repackedCollection = tmpPrefix + "-gov2-repacked";
    String gov2MappingFile = tmpPrefix + "-gov2-mapping-repacked.dat";
    RepackTrecWebCollection.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-collection=" + gov2Path, "-output=" + repackedCollection, "-compressionType=block" });

    TrecWebDocnoMappingBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + repackedCollection,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + gov2MappingFile });

    Gov2DocnoMapping mapping = new Gov2DocnoMapping();
    mapping.loadMapping(new Path(gov2MappingFile), fs);

    assertEquals("GX000-00-0000000", mapping.getDocid(1));
    assertEquals("GX210-38-0737901", mapping.getDocid(20000000));

    assertEquals(1, mapping.getDocno("GX000-00-0000000"));
    assertEquals(20000000, mapping.getDocno("GX210-38-0737901"));
  }

  // wt10g, original.
  @Test
  public void testWt10gDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(wt10gPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String wt10gMappingFile = tmpPrefix + "-wt10g-mapping.dat";
    TrecWebDocnoMappingBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + wt10gPath,
        "-" + DocnoMapping.BuilderUtils.FORMAT_OPTION + "=" + TrecWebDocumentInputFormat.class.getCanonicalName(),
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + wt10gMappingFile });

    Wt10gDocnoMapping mapping = new Wt10gDocnoMapping();
    mapping.loadMapping(new Path(wt10gMappingFile), fs);

    assertEquals("WTX001-B01-1", mapping.getDocid(1));
    assertEquals("WTX062-B34-37", mapping.getDocid(1000000));

    assertEquals(1, mapping.getDocno("WTX001-B01-1"));
    assertEquals(1000000, mapping.getDocno("WTX062-B34-37"));
  }

  // gov2, original.
  @Test
  public void testGov2DocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(gov2Path));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09-jarjar"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String gov2MappingFile = tmpPrefix + "-gov2-mapping.dat";
    TrecWebDocnoMappingBuilder.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + gov2Path,
        "-" + DocnoMapping.BuilderUtils.FORMAT_OPTION + "=" + TrecWebDocumentInputFormat.class.getCanonicalName(),
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + gov2MappingFile });

    Gov2DocnoMapping mapping = new Gov2DocnoMapping();
    mapping.loadMapping(new Path(gov2MappingFile), fs);

    assertEquals("GX000-00-0000000", mapping.getDocid(1));
    assertEquals("GX210-38-0737901", mapping.getDocid(20000000));

    assertEquals(1, mapping.getDocno("GX000-00-0000000"));
    assertEquals(20000000, mapping.getDocno("GX210-38-0737901"));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
