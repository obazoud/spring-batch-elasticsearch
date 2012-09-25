package org.bazoud.batch;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.BeforeClass;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author @obazoud (Olivier Bazoud)
 */
public class StartNode {
  protected static Node node;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if (node == null) {
      // We remove old data before launching tests
      removeOldDataDir();

      // Then we start our node for tests
      node = NodeBuilder.nodeBuilder().node();

      // We wait now for the yellow (or green) status
      node.client().admin().cluster().prepareHealth()
              .setWaitForYellowStatus().execute().actionGet();

      assertNotNull(node);
      assertFalse(node.isClosed());
    }
  }

  private static void removeOldDataDir() throws Exception {
    Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath("elasticsearch.yml").build();

    // First we delete old datas...
    File dataDir = new File(settings.get("path.data"));
    if (dataDir.exists()) {
      FileSystemUtils.deleteRecursively(dataDir, true);
    }
  }
}
