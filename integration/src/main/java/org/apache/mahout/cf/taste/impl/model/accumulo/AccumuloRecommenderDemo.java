package org.apache.mahout.cf.taste.impl.model.accumulo;

import java.io.File;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.iterator.FileLineIterable;

public class AccumuloRecommenderDemo {
	public static void main(String[] args) throws Exception {

		if(args.length != 6)
		{
			System.err.println("Usage: java "+AccumuloRecommenderDemo.class.getName()+" <instance> <zookeepers> <user> <password> <table> <file>");
			System.exit(-1);
		}
		
		int i = 0;
		String inst = args[i++];
		String zk = args[i++];
		String user = args[i++];
		String pass = args[i++];;
		String table = args[i++];;
		String file = args[i++];;
		
		AccumuloDataModel model = 
				new AccumuloDataModel(inst, zk, user, pass, table, new Authorizations());
		
		long count = 0;
		for (String line : new FileLineIterable(new File(file))) {
			String[] fields = line.split("\t");
			long userID = Long.parseLong(fields[0]);
			long itemID = Long.parseLong(fields[1]);
			float rating = Float.parseFloat(fields[2]);
			model.setPreference(userID, itemID, rating);
			++count;
			if(count % 10000 == 0)
			{
				System.out.println("Ingested "+count+" records ...");
			}
		}
		model.flush();

		System.out.println("Finished loading");
		try {
			UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
			NearestNUserNeighborhood neighborhood = new NearestNUserNeighborhood(10, similarity,
					model);

			Recommender recommender = new GenericUserBasedRecommender(model, neighborhood,
					similarity);
			System.out.println(recommender.recommend(101, 10));
			System.out.println(recommender.recommend(102, 10));
			System.out.println(recommender.recommend(103, 10));
		} finally {

			model.close();
		}
	}
}
