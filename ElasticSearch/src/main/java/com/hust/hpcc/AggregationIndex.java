package com.hust.hpcc;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;

public class AggregationIndex {

	// aggregation
	public static void aggregations(Client client, String index, String type,
			String field) {

		/*
		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.max("max_agg").field(field))
				.execute().actionGet();
		*/

		
		// Terms
		/*
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
		}
		*/
		
		// Percentiles
		/*
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
		}
		*/

		// Avg nest term
		/*
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
		for (Terms.Bucket entry : agg.getBuckets()) {
			String key = entry.getKey();
			long count = entry.getDocCount();
			Avg avg = entry.getAggregations().get("avg_name");
			System.out.println("Key[" + key + "] : Count[" + count + "] : Avg["
					+ avg.getValue() + "]");
		}
		*/

		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders.geoDistance("geo").field(field)
								.point(new GeoPoint("15, 105"))
								.unit(DistanceUnit.KILOMETERS).addUnboundedTo(10000)
								.addRange(10000, 1200000)
								.addUnboundedFrom(1200000)).execute()
				.actionGet();
		System.out.println(response);

		GeoDistance agg = response.getAggregations().get("geo");
		for (GeoDistance.Bucket entry : agg.getBuckets()) {
			String key = entry.getKey();
			Number from = entry.getFrom();
			Number to = entry.getTo();
			long count = entry.getDocCount();
			System.out.println("Key[" + key + "] : From[" + from + "] : To["
					+ to + "] : Count[" + count + "]");
		}
	}
	
	public static void main(String[] args) {
		Node node = NodeBuilder.nodeBuilder().clusterName("TuyenNG").node();
		Client client = node.client();
		aggregations(client, "elastic", "geo", "nothing");
		node.close();
	}

}
