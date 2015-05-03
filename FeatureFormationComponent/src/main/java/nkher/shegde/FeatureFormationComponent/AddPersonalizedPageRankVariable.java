package nkher.shegde.FeatureFormationComponent;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/***
 * 
 * Adds the the user's personalized page rank score for a user to the final data set. 
 * Logic is explained in the addPersonalizedPageRankVariable() function.
 * 
 * @author nameshkher, Sarika Hegde
 * 
 */
public class AddPersonalizedPageRankVariable {
	
	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	// private DBObject dbObject;
	
	/***
	 * Creates a connection with the database
	 * m
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
	 *  Adds the Personalized Page Rank Variable to the data set
	 *  
	 *  @param userPageRanks - Hash map of users and their page ranks
	 * @throws UnknownHostException 
	 */
	public void addPersonalizedPageRankVariable(HashMap<String, Double> userPageRanks, String collectionName) throws UnknownHostException {
		System.out.println("Updating collection " + collectionName + " with Personalized Page rank values for all users.");
		setupConnection("classification_data");
		getCollection(collectionName);
		dbCursor = collection.find();
		try {
			while (dbCursor.hasNext()) {
				DBObject row = dbCursor.next();
				String userId = row.get("user_id").toString();
				double personalizedPageRank = userPageRanks.get(userId);
				
				
				// Update the classification data with the personalized page rank values
				DBObject personalized_pagerank = new BasicDBObject().append("$set", new BasicDBObject("Personalized Page Rank", personalizedPageRank));
				collection.update(row, personalized_pagerank);
			}
			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error while writing the user page ranks to the file.");
		}
		System.out.println("Collection is updated. Please check :)");
	}
	
	public static void main(String[] args) throws IOException {
		
		StorePersonalizedPageRankScores object = new StorePersonalizedPageRankScores();
		String readPath = "/Users/nameshkher/Documents/Semester_IV/Big_Data_Insights", readFileName = "Personalized_PR_AllUsers";
		object.readPageRanks(readPath, readFileName);
		
		HashMap<String, Double> userBusinessMap = object.getUserPageRanksHashMap();
		
		AddPersonalizedPageRankVariable obj = new AddPersonalizedPageRankVariable();
		obj.addPersonalizedPageRankVariable(userBusinessMap, "cd7");
	}

}
