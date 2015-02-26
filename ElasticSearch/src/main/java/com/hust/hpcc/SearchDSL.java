package com.hust.hpcc;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

public class SearchDSL {

	// print
	public static void printDocument(Map<String, Object> result) {
		for (String str : result.keySet()) {
			System.out.println(str + " : " + result.get(str));
		}
	}

	@SuppressWarnings("unused")
	private static String fieldQuery(String field, String value) {
		String query = "{\"term\": {\"" + field + "\":\"" + value + "\"}}";
		String q = "{\"bool\":{\"must\":{\"range\":{\"age\":{\"from\":21, \"to\":30}}}}}";
		String distance = "{\"filtered\": {\"query\":{\"match_all\":{}}}}";
		System.out.println(distance);
		return distance;
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

	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		searchDSL(client, "bank", "account", "lastname", "fox");
		node.close();
	}
	
}
