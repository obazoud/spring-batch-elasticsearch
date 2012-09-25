package org.bazoud.batch.elasticsearch.tasklet;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * @author @obazoud (Olivier Bazoud)
 */
public class CreateElasticsearchIndexTasklet implements Tasklet, InitializingBean {
  private Client client;
  private String indexName;
  private String typeName;

  @Override
  public void afterPropertiesSet() throws Exception {
    Assert.notNull(client, "client must not be null");
    Assert.notNull(indexName, "indexName must not be null");
    Assert.notNull(typeName, "typeName must not be null");
  }

  @Override
  public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
    try {
      // client.admin().indices().create(new CreateIndexRequest((indexName, new Settings(""))));
      client.admin().indices().prepareCreate(indexName).execute().actionGet();
    } catch (Exception e) {
      if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
        // that's fine
      } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
        // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
      } else {
        throw new RuntimeException("failed to create index " + indexName, e);
      }
    }
    return RepeatStatus.FINISHED;
  }

  public Client getClient() {
    return client;
  }

  public void setClient(Client client) {
    this.client = client;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }
}
