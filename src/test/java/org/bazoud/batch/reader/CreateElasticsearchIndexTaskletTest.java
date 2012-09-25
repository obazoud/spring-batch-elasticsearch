package org.bazoud.batch.reader;

import org.bazoud.batch.StartNode;
import org.bazoud.batch.elasticsearch.tasklet.CreateElasticsearchIndexTasklet;
import org.junit.Test;
import org.springframework.batch.repeat.RepeatStatus;

import static org.junit.Assert.assertEquals;

/**
 * @author @obazoud (Olivier Bazoud)
 */
public class CreateElasticsearchIndexTaskletTest extends StartNode {

  @Test
  public void createIndex() throws Exception {
    CreateElasticsearchIndexTasklet tasklet = new CreateElasticsearchIndexTasklet();
    tasklet.setClient(node.client());
    tasklet.setIndexName("myindex");
    tasklet.setTypeName("mytype");
    tasklet.afterPropertiesSet();

    RepeatStatus repeatStatus = tasklet.execute(null, null);
    assertEquals(repeatStatus, RepeatStatus.FINISHED);
  }
}