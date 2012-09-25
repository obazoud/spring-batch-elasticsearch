package org.bazoud.batch.elasticsearch.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.springframework.batch.classify.Classifier;
import org.springframework.batch.classify.ClassifierSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.List;

import static org.elasticsearch.client.Requests.indexRequest;

/**
 * @author @obazoud (Olivier Bazoud)
 */
public class ElasticseachItemWriter<T> implements ItemWriter<T>, InitializingBean {
  private Client client;
  private String indexName;
  private String typeName;
  private Classifier<T, String> idClassifier;
  private Classifier<T, String> typeNameClassifier;
  private Classifier<T, String> indexNameClassifier;
  private Classifier<T, ActionRequest> actionRequestClassifier;
  private ObjectMapper mapper;
  private long timeoutMillis;

  @Override
  public void afterPropertiesSet() throws Exception {
    Assert.notNull(client, "client must not be null");
    Assert.notNull(mapper, "mapper must not be null");
    if (typeName == null) {
      typeNameClassifier = new ClassifierSupport<T, String>(typeName);
    }
    if (indexNameClassifier == null) {
      indexNameClassifier = new ClassifierSupport<T, String>(indexName);
    }
    if (actionRequestClassifier == null) {
      // TODO: improve this!
      actionRequestClassifier = new ClassifierSupport<T, ActionRequest>(new IndexRequest(indexName));
    }
    if (idClassifier == null) {
      idClassifier = new ClassifierSupport<T, String>(null);
    }
  }

  @Override
  public void write(List<? extends T> items) throws Exception {
    BulkRequestBuilder bulk = client.prepareBulk();

    for(T item: items) {
      bulk.request().add(actionRequestClassifier.classify(item));
      bulk.add(indexRequest(indexNameClassifier.classify(item))
        .type(typeNameClassifier.classify(item))
        .id(idClassifier.classify(item))
        .source(transform(item)));
    }

    BulkResponse response = bulk.execute().actionGet(timeoutMillis);
    if (response.hasFailures()) {
      throw new RuntimeException(response.buildFailureMessage());
    }
  }

  protected String transform(T item) throws Exception {
    return mapper.writeValueAsString(item);
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

  public Classifier<T, String> getIdClassifier() {
    return idClassifier;
  }

  public void setIdClassifier(Classifier<T, String> idClassifier) {
    this.idClassifier = idClassifier;
  }

  public Classifier<T, String> getTypeNameClassifier() {
    return typeNameClassifier;
  }

  public void setTypeNameClassifier(Classifier<T, String> typeNameClassifier) {
    this.typeNameClassifier = typeNameClassifier;
  }

  public Classifier<T, String> getIndexNameClassifier() {
    return indexNameClassifier;
  }

  public void setIndexNameClassifier(Classifier<T, String> indexNameClassifier) {
    this.indexNameClassifier = indexNameClassifier;
  }

  public Classifier<T, ActionRequest> getActionRequestClassifier() {
    return actionRequestClassifier;
  }

  public void setActionRequestClassifier(Classifier<T, ActionRequest> actionRequestClassifier) {
    this.actionRequestClassifier = actionRequestClassifier;
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}
