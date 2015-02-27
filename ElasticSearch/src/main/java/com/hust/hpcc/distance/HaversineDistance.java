package com.hust.hpcc.distance;

public class HaversineDistance {

	public static final int R = 6371;

	private static Double toRad(Double value) {
		return value * Math.PI / 180;
	}

	public static void main(String[] args) {
		Location loc1 = new Location(20.9408321,106.3244284);
		Location loc2 = new Location(21.0226967,105.8369637);
		double latDistance = toRad(loc1.getLatitude() - loc2.getLatitude());
		double lonDistance = toRad(loc1.getLongitude() - loc2.getLongitude());
		Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
				+ Math.cos(toRad(loc1.getLatitude()))
				* Math.cos(toRad(loc2.getLatitude()))
				* Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		Double distance = R * c;
		System.out.println("The distance between " + loc1.toString() + " and "
				+ loc2.toString() + " is:\n" + distance);
	}
}
