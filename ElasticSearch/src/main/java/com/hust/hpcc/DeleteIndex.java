package com.hust.hpcc;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class DeleteIndex {

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
	
	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		delete(client, "test", "type", "1");
		node.close();
	}
}
