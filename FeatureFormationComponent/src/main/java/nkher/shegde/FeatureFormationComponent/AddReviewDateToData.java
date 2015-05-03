package nkher.shegde.FeatureFormationComponent;

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
 * Adds the the social sentiment score for a user, business combination to the final data set
 * which is user for analysis. Logic is explained in the addSocialSentimentScore() function.
 * 
 * @author nameshkher, Sarika Hegde
 * 
 */
public class AddReviewDateToData {
	
	public static final int MAX_SIZE = 100000;
	
	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	private DBObject dbObject;
	
	private HashMap<String, String> review_Id_Map = new HashMap<String, String>();
	
	public void setupConnection(String dbName) throws UnknownHostException {
		mClient = new MongoClient();
		yelpDB = mClient.getDB(dbName);
	}
	
	public void getCollection(String collectionName) {
		collection = yelpDB.getCollection(collectionName);
	}
	
	public void closeAll() {
		mClient.close();
		yelpDB.cleanCursors(true);
		collection = null;
	}
	
	/***
	 * 
	 * Sets up a new connection with the passed database and returns the collection passed
	 * 
	 * @param Database Name
	 * @param Collection Name
	 * @return
	 * @throws UnknownHostException
	 */
	public DBCollection creatNewConnection(String dbName, String collectionName) throws UnknownHostException {
		MongoClient client = new MongoClient();
		DB database = client.getDB(dbName);
		DBCollection coll = database.getCollection(collectionName);
		return coll;
	}
	
	
	/***
	 * 
	 * Creates a Hash Map of all review id's and their review dates
	 * @throws UnknownHostException
	 */
	public void fillHashMapReviewDates() throws UnknownHostException {
		
		System.out.println("Collecting all review ids and their dates ");
		setupConnection("yelp");
		getCollection("review");
		dbCursor = collection.find();		
		try {
			while (dbCursor.hasNext()) {
				dbObject = dbCursor.next();
				String reviewId = dbObject.get("review_id").toString();
				String date = dbObject.get("date").toString();
				review_Id_Map.put(reviewId, date);
			}
		}
		catch(Exception e) {
			System.out.println("Some error occurred when collecting reviewed ids and their dates.");
			closeAll();
		}
		System.out.println("Done collecting.");
	}
	
	/***
	 * Takes in a collection name and adds the review date for that 
	 * review using the review_Id_Map (hash map), which is filled
	 * in the fillHashMapReviewDates() method. The fillHashMapReviewDates()
	 * method must be called before this for the function to work properly.
	 * 
	 * @param collectionName
	 * @throws UnknownHostException
	 */
	public void addReviewDates(String collectionName) throws UnknownHostException {
		
		System.out.println("Adding reviewed date to dataset... ");
		setupConnection("classification_data");
		getCollection(collectionName);
		dbCursor = collection.find();
				
		int count = 0;
		try {
			while (dbCursor.hasNext()) {
				DBObject row = dbCursor.next();
				
				count = count+1;
				
				String reviewId = null;
				String revDate = "";
				
				if (row.get("review_id") != null) {
					reviewId = row.get("review_id").toString();
				}
				else {
					reviewId = "null";
				}
				
				if (review_Id_Map.containsKey(reviewId)) {
					revDate = review_Id_Map.get(reviewId);
				} else {
					revDate = "NA";
				}
								
				// Update the classification data with the social influence factor
				DBObject reviewedDate = new BasicDBObject().append("$set", new BasicDBObject("reviewed_date", revDate));
				collection.update(row, reviewedDate);
								
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Some error occured when accessing the classification data");
		}
		System.out.println("Done adding for ... " + count + " rows");
	}
	
	public static void main(String[] args) throws UnknownHostException {
		AddReviewDateToData object = new AddReviewDateToData();
		object.fillHashMapReviewDates();
		System.out.println("Size of hashmap : " + object.review_Id_Map.size());
		
		object.addReviewDates("cd7");
	}

}
