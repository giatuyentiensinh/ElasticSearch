package com.hust.hpcc;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;

public class HelloElasticSearch {

	// create
	public static void createIndex(Client client, String index, String type,
			String id, String name, String content, Date postDate) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", name);
		map.put("content", content);
		map.put("date", postDate);
		IndexResponse response = client.prepareIndex(index, type, id)
				.setSource(map).execute().actionGet();
		System.out.println("index  : " + response.getIndex());
		System.out.println("type   : " + response.getType());
		System.out.println("id     : " + response.getId());
		System.out.println("version: " + response.getVersion());
	}

	// print
	public static void printDocument(Map<String, Object> result) {
		for (String str : result.keySet()) {
			System.out.println(str + " : " + result.get(str));
		}
	}

	// delete
	public static void delete(Client client, String index, String type,
			String id) {
		DeleteResponse response = client.prepareDelete(index, type, id)
				.execute().actionGet();
		System.out.println("Information on the deleted document");
		System.out.println("index  : " + response.getIndex());
		System.out.println("type   : " + response.getType());
		System.out.println("id     : " + response.getId());
		System.out.println("version: " + response.getVersion());
	}

	// update
	public static void update(Client client, String index, String type,
			String id, String field, String newValue) {

		/*
		 * UpdateResponse response = client .prepareUpdate(index, type, id)
		 * .setScript("ctx._source." + field + "=\"" + newValue + "\"",
		 * ScriptType.INLINE).get();
		 * System.out.println("Information on the update document");
		 * System.out.println("index  : " + response.getIndex());
		 * System.out.println("type   : " + response.getType());
		 * System.out.println("id     : " + response.getId());
		 * System.out.println("version: " + response.getVersion());
		 */
		try {
			UpdateRequest request = new UpdateRequest(index, type, id)
					.script("ctx._source." + field + "=\"" + newValue + "\"");
			client.update(request).get();
			System.out.println("Information on the update document");
			System.out.println("index  : " + request.index());
			System.out.println("type   : " + request.type());
			System.out.println("id     : " + request.id());
			System.out.println("version: " + request.version());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// search
	public static void search(Client client, String index, String type,
			String field, String value) {
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.termQuery(field, value)).execute()
				.actionGet();
		// SearchResponse response = client.prepareSearch(index).setSize(20)
		// .execute().actionGet();
		SearchHit[] result = response.getHits().getHits();
		System.out.println("Information on the search document");
		System.out.println("Current result: " + result.length);
		for (SearchHit hit : result) {
			System.out.println("---------------------------------------");
			printDocument(hit.getSource());
		}
		System.out.println("---------------------------------------");
	}

	// aggregation
	public static void aggregations(Client client, String index, String type,
			String field) {

		/*SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.max("max_agg").field(field))
				.execute().actionGet();*/

		/*
		 // Terms
		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders.terms("name_term")
								.field("lastname")).execute().actionGet();
		Terms agg = response.getAggregations().get("name_term");
		for (Terms.Bucket entry : agg.getBuckets()) {
			String key = entry.getKey();
			long docCount = entry.getDocCount();
			System.out.println("Key[" + key + "] : " + "Count[" + docCount
					+ "]");
		}*/
		
		/*
		 // Percentiles
		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders.percentiles("address").field("age")
								.percentiles(20, 22, 40)).execute().actionGet();
		Percentiles agg = response.getAggregations().get("address");
		for (Percentile entry : agg) {
			double percent = entry.getPercent();
			double value = entry.getValue();
			System.out.println("Percent[" + percent + "] : Value[" + value
					+ "]");
		}*/
		
		/*
		// Avg nest term
		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders
								.terms("term_name")
								.field("gender")
								.subAggregation(
										AggregationBuilders.avg("avg_name")
												.field("age"))).setSize(0)
				.execute().actionGet();
		System.out.println(response);
		System.out.println("------------------------------");
		Terms agg = response.getAggregations().get("term_name");
		for(Terms.Bucket entry : agg.getBuckets()) {
			String key = entry.getKey();
			long count = entry.getDocCount();
			Avg avg = entry.getAggregations().get("avg_name");
			System.out.println("Key[" + key + "] : Count[" + count + "] : Avg["
					+ avg.getValue() + "]");
		}*/
		
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.geoDistance("geo").field(field).
						point(new GeoPoint("15, 105")).unit(DistanceUnit.YARD)
						.addUnboundedTo(10000)
						.addRange(10000, 1200000)
						.addUnboundedFrom(1200000))
				.execute().actionGet();
		System.out.println(response);
		
		GeoDistance agg = response.getAggregations().get("geo");
		for(GeoDistance.Bucket entry: agg.getBuckets()) {
			String key = entry.getKey();
			Number from = entry.getFrom();
			Number to = entry.getTo();
			long count = entry.getDocCount();
			System.out.println("Key[" + key + "] : From[" + from + "] : To["
					+ to + "] : Count[" + count +"]");
		}
	}

	// search DSL
	public static void searchDSL(Client client, String index, String type,
			String field, String value) {
		SearchResponse response = client.prepareSearch(index)
				.setQuery(fieldQuery(field, value)).setSize(5).execute()
				.actionGet();
		SearchHit[] result = response.getHits().getHits();
		System.out.println("Information on the search document");
		System.out.println("Current result: " + result.length);
		for (SearchHit hit : result) {
			System.out.println("---------------------------------------");
			printDocument(hit.getSource());
		}
		System.out.println("---------------------------------------");
	}

	@SuppressWarnings("unused")
	private static String fieldQuery(String field, String value) {
		String query = "{\"term\": {\"" + field + "\":\"" + value + "\"}}";
		String q = "{\"bool\":{\"must\":{\"range\":{\"age\":{\"from\":21, \"to\":30}}}}}";
		String distance = "{\"filtered\": {\"query\":{\"match_all\":{}}}}";
		System.out.println(distance);
		return distance;
	}

	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		String index = "elastic";
		String type = "geo";
		 String field = "age";
		// String id = "1";
		// fieldQuery("name", "Tuyen");
		// createIndex(client, "test", "type", "1", "Cao Hang", "My Friend", new
		// Date());

		// GetResponse getResponse = client.prepareGet(index, type,
		// id).execute()
		// .actionGet();
		// Map<String, Object> map = getResponse.getSource();
		// printDocument(map);

		// search(client, index, type, "lastname", "haney");
		aggregations(client, index, type, field);
		// searchDSL(client, index, type, field, "20");
		node.close();
	}
}
