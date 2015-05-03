package nkher.shegde.FeatureFormationComponent;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/***
 * Adds the the social influence score for a user, business combination to the final data set
 * which is user for analysis. Logic is explained in the addAverageSocialInfluence() function.
 * 
 * @author nameshkher, Sarika Hegde
 */


public class AddSocialInfluenceVariable {

	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	private DBObject dbObject;
	private HashMap<String, HashSet<String>> userBusinessMap = new HashMap<String, HashSet<String>>();
	
	/***
	 * Creates a connection with the database
	 * 
	 * @param database name
	 * @throws UnknownHostException
	 * 
	 */
	public void setupConnection(String dbName) throws UnknownHostException {
		mClient = new MongoClient();
		yelpDB = mClient.getDB(dbName);
	}
	
	/***
	 * 
	 * Gets the collection from the specified database passed in the setupConnection() method
	 * 
	 * @param collectionName
	 */
	public void getCollection(String collectionName) {
		collection = yelpDB.getCollection(collectionName);
	}
	
	/***
	 * Closes the connection with the Mongo client
	 * 
	 */
	public void closeAll() {
		mClient.close();
		yelpDB.cleanCursors(true);
		collection = null;
	}
	
	/***
	 * Builds a hash map of business id's (keys) and another hash set of users as its value.
	 * The hash set contains all the users who have reviewed for that business.
	 * 
	 * @throws UnknownHostException
	 */
	public void fillUserBusinessMap() throws UnknownHostException {
		setupConnection("yelp");
		getCollection("review");
		dbCursor = collection.find();
		try {
			while (dbCursor.hasNext()) {
				dbObject = dbCursor.next();
				String business_id = dbObject.get("business_id").toString();
				String user_id = dbObject.get("user_id").toString();
				if (!userBusinessMap.containsKey(business_id)) {
					userBusinessMap.put(business_id, new HashSet<String>());
				}
				HashSet<String> user_ids = userBusinessMap.get(business_id);
				user_ids.add(user_id); // Add the use id to the set
				userBusinessMap.put(business_id, user_ids); // Add the set back to the hash-map				
			}
		}
		catch (Exception e) {
			System.out.println("Some exception occured while accessing the review JSON file");
		}
		finally {
			closeAll();
			dbCursor.close();	
		}
		System.out.println("Got businesses that have reviews and also users that have given atleast one review for it.\n");
	}
	
	/***
	 * Method adds a variable named social influence to each document in 
	 * the passed collection in database classification_data. The social 
	 * influence is formed by taking a 1 if a user's friend has given a review
	 * else 0, which is then summed up and averaged.
	 * 
	 * @throws UnknownHostException
	 */
	public void addAverageSocialInfluence() throws UnknownHostException {
		setupConnection("classification_data");
		getCollection("cd7");
		dbCursor = collection.find();
		try {
			while (dbCursor.hasNext()) {
				DBObject row = dbCursor.next();
				double avg_social_influence = 0;
				String businessId = row.get("business_id").toString();
				@SuppressWarnings("unchecked")
				ArrayList<String> friends = (ArrayList<String>) row.get("friends");
				int numberOfFriends = friends.size();
				for (String friend : friends) {
					// Check if friend has reviewed for that business
					if (userBusinessMap.get(businessId).contains(friend)) {
						avg_social_influence += 1;
					}
				}
				avg_social_influence = (double)(avg_social_influence/numberOfFriends);
				
				// Update the classification data with the social influence factor
				DBObject socialInfluence = new BasicDBObject().append("$set", new BasicDBObject("Social Influence", avg_social_influence));
				collection.update(row, socialInfluence);				
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Some exception occured while accessing the review JSON file");
		}
		finally {
			dbCursor.close();	
		}
	}
	
	public static void main(String[] args) throws UnknownHostException {
		AddSocialInfluenceVariable object = new AddSocialInfluenceVariable();
		
		System.out.println("Filling the user business map from the review collection in the yelp database.");
		object.fillUserBusinessMap();
		System.out.println("User business map filled.");
		
		System.out.println("Adding the social influence factor to the classification dataset ... ");
		object.addAverageSocialInfluence();
		System.out.println("Done adding. :)");
	}

}
