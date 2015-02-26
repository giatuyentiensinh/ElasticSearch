package com.hust.hpcc;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class CreateIndex {

	// print
	public static void printDocument(Map<String, Object> result) {
		for (String str : result.keySet()) {
			System.out.println(str + " : " + result.get(str));
		}
	}

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

	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		createIndex(client, "test", "type", "1", "Cao Hang", "My Friend",
				new Date());
		GetResponse getResponse = client.prepareGet("test", "type", "1")
				.execute().actionGet();
		Map<String, Object> map = getResponse.getSource();
		printDocument(map);
		node.close();
	}

}
