package com.hust.hpcc;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

public class SearchDocument {

	// print
	public static void printDocument(Map<String, Object> result) {
		for (String str : result.keySet()) {
			System.out.println(str + " : " + result.get(str));
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

	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		search(client, "bank", "account", "age", "20");
		node.close();
	}

}
