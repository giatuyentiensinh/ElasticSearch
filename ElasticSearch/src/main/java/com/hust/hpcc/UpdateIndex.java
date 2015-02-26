package com.hust.hpcc;

import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class UpdateIndex {

	// update
	public static void update(Client client, String index, String type,
			String id, String field, String newValue) {
		/*
		UpdateResponse response = client
				.prepareUpdate(index, type, id)
				.setScript("ctx._source." + field + "=\"" + newValue + "\"",
						ScriptType.INLINE).get();
		System.out.println("Information on the update document");
		System.out.println("index  : " + response.getIndex());
		System.out.println("type   : " + response.getType());
		System.out.println("id     : " + response.getId());
		System.out.println("version: " + response.getVersion());
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

	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		update(client, "test", "type", "1", "name", "TuyenNG");
		node.close();
	}
}
