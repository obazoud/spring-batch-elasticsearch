package org.bazoud.batch.elasticsearch.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author @obazoud (Olivier Bazoud)
 *
 * The implementation is <b>not</b> thread-safe.
 */
public class ElasticseachItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements InitializingBean {
  private Client client;
  private String[] indices;
  private ObjectMapper mapper;

  private QueryBuilder queryBuilder;
  private Class<T> classType;

  private long timeoutMillis;

  private boolean initialized = false;
  private Iterator<SearchHit> hits;

  @Override
  public void afterPropertiesSet() throws Exception {
    Assert.notNull(client, "client must not be null");
    Assert.notNull(mapper, "mapper must not be null");
    Assert.notNull(queryBuilder, "queryBuilder must not be null");
    Assert.notNull(classType, "classType must not be null");
  }

  @Override
  protected T doRead() throws Exception {
    if (hits.hasNext()) {
      return transform(hits.next());
    }
    return null;
  }

  @Override
  protected void doOpen() throws Exception {
    Assert.state(!initialized, "Cannot open an already opened ItemReader, call close first");
    SearchResponse searchResponse = client.prepareSearch(indices).setQuery(queryBuilder).execute().actionGet(timeoutMillis);
    hits = searchResponse.getHits().iterator();
    initialized = true;
  }

  @Override
  protected void doClose() throws Exception {
    initialized = false;
  }

  protected T transform(SearchHit hit) throws IOException {
    return (T) mapper.readValue(hit.getSourceAsString().getBytes(), classType.getClass());
  }

  public Client getClient() {
    return client;
  }

  public void setClient(Client client) {
    this.client = client;
  }

  public String[] getIndices() {
    return indices;
  }

  public void setIndices(String[] indices) {
    this.indices = indices;
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public QueryBuilder getQueryBuilder() {
    return queryBuilder;
  }

  public void setQueryBuilder(QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  public Class<T> getClassType() {
    return classType;
  }

  public void setClassType(Class<T> classType) {
    this.classType = classType;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}
