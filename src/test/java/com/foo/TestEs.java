package com.foo;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;



public class TestEs {
	
	TransportClient transportClient = new TransportClient();
	@Before
	public void test0() throws Exception {
		TransportAddress transportAddress = new InetSocketTransportAddress("192.168.64.143", 9300);
		transportClient.addTransportAddresses(transportAddress);
	}
	
	
	/**
	 * 获取操作es的链接
	 * 工作中建议设置cluster.name和client.transport.sniff  并且把最开始集群内的节点信息尽可能的都添加到transportClient里面
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		Settings settings = ImmutableSettings
						.settingsBuilder()
						.put("cluster.name", "elasticsearch")//如果集群名称被修改了，必须显式指定集群名称
						.put("client.transport.sniff", true)//表示开启集群的嗅探功能,可以自动发现集群内的所有节点信息
						.build();
		//创建一个客户端链接
		TransportClient transportClient = new TransportClient(settings);
		//指定节点信息
		TransportAddress transportAddress = new InetSocketTransportAddress("192.168.64.143", 9300);
		TransportAddress transportAddress1 = new InetSocketTransportAddress("192.168.64.143", 9300);
		
		//添加节点信息
		transportClient.addTransportAddresses(transportAddress);
		
		//获取client链接到的节点信息
		ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println(discoveryNode.getHostAddress());
		}
	}
	
	
	String index = "foo";
	String type = "emp";
	/**
	 * 建立索引-1 json
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		String jsonStr = "{\"name\":\"tom\",\"age\":19}";
		//IndexResponse response = transportClient.prepareIndex(index, type, "50").setSource(jsonStr).execute().actionGet();
		IndexResponse response = transportClient.prepareIndex(index, type, "50")//设置索引库信息
				.setSource(jsonStr)//设置内容
				.get();//执行请求并获取返回内容
		System.out.println(response.getVersion());
	}
	
	/**
	 * 建立索引-2 map
	 * @throws Exception
	 */
	@Test
	public void test3() throws Exception {
		HashMap<String, Object> hashMap = new HashMap<String,Object>();
		hashMap.put("name", "skl");
		hashMap.put("age", 20);
		
		IndexResponse response = transportClient.prepareIndex(index, type, "51").setSource(hashMap).get();
		System.out.println(response.getVersion());
	}
	
	/**
	 * 工作中最常见
	 * 建立索引-3 javabean对象 实体类
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
		Person person = new Person();
		person.setName("haha");
		person.setAge(18);
		
		ObjectMapper objectMapper = new ObjectMapper();
		IndexResponse response = transportClient.prepareIndex(index, type, "52").setSource(objectMapper.writeValueAsString(person)).get();
		System.out.println(response.getVersion());
	}
	
	
	/**
	 * 工作中最常见
	 * 建立索引-4 helper
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		XContentBuilder builder = XContentFactory
			.jsonBuilder()//获取一个json格式的数据
			.startObject()//{
				.field("name","zs")//"name":"zs"
				.field("age", 28)
			.endObject();//}
		
		IndexResponse response = transportClient.prepareIndex(index, type, "53").setSource(builder).get();
		System.out.println(response.getVersion());
	}
	
	
	/**
	 * 查询id查询
	 * get
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		GetResponse response = transportClient.prepareGet(index, type, "53").get();
		System.out.println(response.getSourceAsString());
	}
	
	
	/**
	 * 局部更新 update
	 * @throws Exception
	 */
	@Test
	public void test7() throws Exception {
		XContentBuilder builder = XContentFactory
				.jsonBuilder()//获取一个json格式的数据
				.startObject()//{
					.field("age", 29)
				.endObject();//}
		UpdateResponse response = transportClient.prepareUpdate(index, type, "53").setDoc(builder).get();
		System.out.println(response.getVersion());
	}
	
	
	/**
	 * 删除 根据id删除
	 * @throws Exception
	 */
	@Test
	public void test8() throws Exception {
		DeleteResponse response = transportClient.prepareDelete(index, type, "53").get();
		System.out.println(response.getVersion());
	}
	
	/**
	 * select count(1) from table [ where a=1 and b=2 ]
	 * count操作
	 * @throws Exception
	 */
	@Test
	public void test9() throws Exception {
		long count = transportClient.prepareCount(index).setTypes(type).get().getCount();
		System.out.println(count);
	}
	
	
	/**
	 * bulk 批量操作
	 * @throws Exception
	 */
	@Test
	public void test10() throws Exception {
		BulkRequestBuilder bulkbuilder = transportClient.prepareBulk();
		
		XContentBuilder builder = XContentFactory
				.jsonBuilder()//获取一个json格式的数据
				.startObject()//{
					.field("name","nana")
					.field("age", 29)
				.endObject();//}
		IndexRequest indexrequest = new IndexRequest(index, type, "60");
		indexrequest.source(builder);
		
		
		DeleteRequest deleteRequest = new DeleteRequest(index, type, "51");
		
		
		bulkbuilder.add(indexrequest);
		bulkbuilder.add(deleteRequest);
		
		
		//执行批量操作
		BulkResponse bulkResponse = bulkbuilder.get();
		if(bulkResponse.hasFailures()){
			BulkItemResponse[] items = bulkResponse.getItems();
			for (BulkItemResponse bulkItemResponse : items) {
				System.out.println(bulkItemResponse.getFailureMessage());//打印失败信息
			}
			
		}else{
			System.out.println("全部OK");
		}
		
	}
	
	/**
	 * 查询类型
	 * @throws Exception
	 */
	@Test
	public void test11() throws Exception {
		SearchResponse searchResponse = transportClient.prepareSearch(index)//指定查询的索引库
				.setTypes(type)//指定查询的类型
				.setQuery(QueryBuilders.matchQuery("name", "zs"))//执行查询条件
				.setSearchType(SearchType.QUERY_THEN_FETCH)//指定查询类型
				.setSize(10)
				.get();
		SearchHits hits = searchResponse.getHits();
		long totalHits = hits.getTotalHits();
		System.out.println("总数："+totalHits);
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	
	
	
	
	
	
	
	
}
