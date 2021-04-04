package com.chuang;

import com.alibaba.fastjson.JSON;
import com.chuang.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ZhenEsApiApplicationTests {

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	@Test
	void testCreateIndex() throws IOException {
		CreateIndexRequest request = new CreateIndexRequest("chuang_index");

		CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

		System.out.println(createIndexResponse);
	}

	@Test
	void testExistIndex() throws IOException {
		GetIndexRequest request = new GetIndexRequest("chuang_index");
		boolean exists = restHighLevelClient.indices().exists(request,RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	@Test
	void testDeleteIndex() throws IOException{
		DeleteIndexRequest request = new DeleteIndexRequest("chuang_index");

		AcknowledgedResponse delete = restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
		System.out.println(delete.isAcknowledged());
	}

	@Test
	void testAddDocument() throws IOException {
		User user = new User("zhenshuang",3);

		IndexRequest request = new IndexRequest("chuang_index");

		//���� put /chuang_index/_doc/1
		request.id("1");
		request.timeout(TimeValue.timeValueSeconds(1));
		request.timeout("1s");

		request.source(JSON.toJSONString(user), XContentType.JSON);

		IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	@Test
	void testIsExists() throws IOException {
		GetRequest getRequest = new GetRequest("chuang_index","1");

		getRequest.fetchSourceContext(new FetchSourceContext(false));
		getRequest.storedFields("_none_");

		boolean exists = restHighLevelClient.exists(getRequest,RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	@Test
	void testGetDocument() throws IOException {
		GetRequest getRequest = new GetRequest("chuang_index","1");
		GetResponse getResponse = restHighLevelClient.get(getRequest,RequestOptions.DEFAULT);
		System.out.println(getResponse.getSourceAsString());
		System.out.println(getResponse);
	}

	@Test
	void testUpdateRequest() throws IOException {
		UpdateRequest updateRequest = new UpdateRequest("chuang_index","1");
		updateRequest.timeout("1s");

		User user = new User("真爽学Java",18);
		updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);

		UpdateResponse updateResponse = restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
		System.out.println(updateResponse.status());
	}

	@Test
	void testDeleteRequest() throws IOException {
		DeleteRequest deleteRequest = new DeleteRequest("chuang_index","1");
		deleteRequest.timeout("1s");

		DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
		System.out.println(deleteResponse.status());
	}

	@Test
	void testBulkRequest() throws IOException {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");

		ArrayList<User> userArrayList = new ArrayList<>();
		userArrayList.add(new User("zhenchuang1",3));
		userArrayList.add(new User("zhenchuang2",3));
		userArrayList.add(new User("zhenchuang3",3));
		userArrayList.add(new User("zhenchuang4",3));
		userArrayList.add(new User("zhenchuang5",3));
		userArrayList.add(new User("zhenchuang6",3));

		for (int i = 0;i < userArrayList.size();i++){
			bulkRequest.add(
					new IndexRequest("chuang_index")
					.id(""+(i+1))
					.source(JSON.toJSONString(userArrayList.get(i)),XContentType.JSON)
			);
		}

		BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures());
	}

	@Test
	void testSearch() throws IOException {
		SearchRequest searchRequest = new SearchRequest("chuang_index");

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","zhenchuang3");

		sourceBuilder.query(termQueryBuilder);
		sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
		System.out.println(JSON.toJSONString(searchResponse.getHits()));
		System.out.println("====================");
		for (SearchHit documentFields : searchResponse.getHits().getHits()){
			System.out.println(documentFields.getSourceAsMap());
		}
	}


}
