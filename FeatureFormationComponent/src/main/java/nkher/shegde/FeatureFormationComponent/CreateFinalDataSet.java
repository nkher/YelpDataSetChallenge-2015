package nkher.shegde.FeatureFormationComponent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;

/***
 * 
 * This program gets a set of businesses and users who have reviewed for those businesses.
 * It cross multiplies between those 2 sets and eventually generates an equal number of positive and negative test cases. 
 * Positive -> Sets "Reviewed = 1" Negative -> Sets "Reviewed = 0".
 * This program is based on our final approach to create our final dataset for classification and regression.
 * 
 * @author Namesh Kher, Sarika Hegde
 * 
 */

public class CreateFinalDataSet {

	public static final int BUFFER_SIZE = 500;
	public int positive_count = 0, negative_count = 0;
	public static final int POSITIVE_NEEDED = 25000;
	public static int NEGATIVE_NEEDED = 25000;
	public static int LIMIT = 0;
	
	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	// private DBObject dbObject;
	private BufferedWriter bWriter;
	private File file;
	private FileWriter fWriter;
	private HashMap<String, HashSet<String>> userBusinessMap = new HashMap<String, HashSet<String>>();
	private Set<String> users = new HashSet<String>();
	private Set<String> reviewedBusinesses = new HashSet<String>();
	private AggregationOutput aggregationOutput;
	private ArrayList<String> outputFormat = new ArrayList<String>();
	
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
	 * Fetches users and business combinations who have reviews (from review collection) and stores in output array, 
     * It also stores the users and businesses in a 2 different set
	 * 
	 * @throws IOException
	 */
	public void writePositiveTestCasesAndFillHashmap() throws IOException {
		
		DBCollection userCollection = creatNewConnection("yelp", "user");
		setupConnection("yelp");
		getCollection("review");
		
		int count = 0;
		
		for (String business_id : reviewedBusinesses) {
			
			DBObject query = QueryBuilder.start("business_id").is(business_id).get();
			dbCursor = (DBCursor) collection.find(query);
			DBObject toWrite = new BasicDBObject();
			DBObject review = null;
			
			int numberOfReviews = 0;
			
			HashSet<String> users;
			if (!userBusinessMap.containsKey(business_id)) {
				userBusinessMap.put(business_id, new HashSet<String>());
			}
			users = userBusinessMap.get(business_id);
			
			try {
				while(dbCursor.hasNext()) {
					review = dbCursor.next();
					
					toWrite = fetchUserDetails(review.get("user_id").toString(), userCollection); // Add user details
					
					// Check to remove dangling nodes
					// If a user is not found in the user file, means it is a dangling node
					// there fore do not consider it
					if (toWrite.get("user_id") == null) {
						continue;
					}
					
					/* Add the user to the main set */
					this.users.add(review.get("user_id").toString());
					users.add(review.get("user_id").toString());
					userBusinessMap.put(business_id, users);
					
					toWrite.put("business_id", business_id); // Add business Id
					toWrite.put("Reviewed", 1); 
					toWrite.put("review_id", review.get("review_id").toString());
					
					if (toWrite.toString().length() < 10) {
						System.out.println("Did not put this in the list for writing " + toWrite.toString());
						continue;
					}
					else outputFormat.add(toWrite.toString());
					
					// Increment the counter
					this.positive_count++; 
					numberOfReviews++;
					
					if (outputFormat.size() == BUFFER_SIZE) {
						System.out.println("Output array reached buffer size. Has to flush");
						flushOutputArray();
					}
					if (this.positive_count == POSITIVE_NEEDED) {
						return;
					}
					if (numberOfReviews == LIMIT) break;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				System.out.println("Some exception occured while accessing the review JSON file or while writing. "
						+ "\n Please print the complete stack trace for more information.");
			}
			finally {
				dbCursor.close();
			}
		}
		System.out.println("Total entries (with Reviewed = 1) written to data file are : " + count);
	} 
	
	/***
	 *  Flushes the output array to a flat file. 
	 * 
	 * @throws IOException
	 */
	public void flushOutputArray() throws IOException {
		file = new File("Output/user_classification");
		fWriter = file.exists() ? new FileWriter(file, true) : new FileWriter(file);
		bWriter = new BufferedWriter(fWriter);
		for (String output : outputFormat) {
			bWriter.write(output);
			bWriter.newLine();
		}
		bWriter.close();
		System.out.println("Flushed output array. Reinitializing it.");
		this.outputFormat = new ArrayList<String>();
	}
	
	
	/***
	 * Takes in a user id and collection name and returns a new DBObject
	 * that contains the users details like 
	 * {user_id, name, fans, review_count, average_stars, yelping_since, friends}
	 * 
	 * @param user 
	 * @param collection 
	 * @return
	 * @throws UnknownHostException
	 */
	public DBObject fetchUserDetails(String user, DBCollection collection) throws UnknownHostException {
		
		DBObject query = QueryBuilder.start("user_id").is(user).get();
		DBCursor dbCursor = (DBCursor) collection.find(query);
		DBObject dbObject = new BasicDBObject();
		DBObject userToReturn = new BasicDBObject();
		
		try {
			while(dbCursor.hasNext()) {
				dbObject = dbCursor.next();
				userToReturn.put("user_id", dbObject.get("user_id"));
				userToReturn.put("name", dbObject.get("name"));
				userToReturn.put("fans", dbObject.get("fans"));
				userToReturn.put("review_count", dbObject.get("review_count"));
				userToReturn.put("average_stars", dbObject.get("average_stars"));
				userToReturn.put("yelping_since", dbObject.get("yelping_since"));
				userToReturn.put("friends", dbObject.get("friends"));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			dbCursor.close();
		}
		
		return userToReturn;
	}
	
	/***
	 *  Gets businesses which have a review count within a given range and to the limit passed (limit, minCount, maxCount)
	 * 
	 * @param lim 
	 * @param minCount 
	 * @param maxCount 
	 * @throws UnknownHostException
	 */
	public void getRangeReviewedBusinessesWithinRange(int lim, int minCount, int maxCount) throws UnknownHostException {
		setupConnection("yelp");
		getCollection("review");
		
		/* Preparing the aggregation pipeline for running the query */
		
		// Apply group operation
		DBObject groupFields  = new BasicDBObject("_id", "$business_id");
		groupFields.put("review_count", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);
		
		// Apply sort operation
		@SuppressWarnings("unused")
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("review_count", -1));
		
		// Apply limit operation
		@SuppressWarnings("unused")
		DBObject limit = new BasicDBObject("$limit", lim);
		
		// Apply match operation
		DBObject query = QueryBuilder.start("review_count").greaterThan(minCount).and("review_count").lessThan(maxCount).get();
		DBObject match = new BasicDBObject("$match", query);
		// match.put("$match", query);
				
		// Run the aggregation
		aggregationOutput = collection.aggregate(group, match);
				
		try {
			for (DBObject result : aggregationOutput.results()) {
				System.out.println(result.get("_id").toString() + " " + result.get("review_count").toString());
				reviewedBusinesses.add(result.get("_id").toString());
			}
		}
		catch (Exception e) {
			System.out.println("Some exception occured while accessing the review JSON file");
		}
		finally {
			closeAll();
		}
	}

	/***
	 *  Gets businesses which have maximum reviews till the limit passed and adds 
	 *  it to the reviewedBusinesses collection
	 * 
	 * @param lim 
	 * @param minCount 
	 * @param maxCount 
	 * @throws UnknownHostException
	 */
	
	public void getRangeReviewedBusinesses(int lim) throws UnknownHostException {
		setupConnection("yelp");
		getCollection("review");
		
		/* Preparing the aggregation pipeline for running the query */
		
		// Apply group operation
		DBObject groupFields  = new BasicDBObject("_id", "$business_id");
		groupFields.put("review_count", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);
		
		// Apply sort operation
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("review_count", -1));
		
		// Apply limit operation
		DBObject limit = new BasicDBObject("$limit", lim);
		
		// Run the aggregation
		aggregationOutput = collection.aggregate(group, sort, limit);
				
		try {
			for (DBObject result : aggregationOutput.results()) {
				reviewedBusinesses.add(result.get("_id").toString());
			}
		}
		catch (Exception e) {
			System.out.println("Some exception occured while accessing the review JSON file");
		}
		finally {
		}
	}
	
	/***
	 * It fetches user and business combinations from user and business hash map filled by
	 * writePositiveTestCasesAndFillHashmap() method and writes samples (user business combinations) 
	 * that do not have any reviews and stores it in output array
	 * Prerequisite : writePositiveTestCasesAndFillHashmap() method should run before this method
	 * 
	 * @throws IOException
	 */
	public void writeUnreviewedTestCases() throws IOException {
		
		if (positive_count < POSITIVE_NEEDED) {
			System.out.println("Got less than " + POSITIVE_NEEDED + " test cases.");
			System.out.println("Changing NEGATIVE_NEEDED from " + NEGATIVE_NEEDED + " to " + positive_count);
			NEGATIVE_NEEDED = positive_count;
		}
		
		int numberOfReviews = 0; // To get equal negative reviews for a business
		setupConnection("yelp");
		getCollection("user");
		
		for (String business_id : reviewedBusinesses) {

			for (String user : this.users) {
								
				if (userBusinessMap.get(business_id) != null && !userBusinessMap.get(business_id).contains(user)) {
					
					DBObject toWrite = new BasicDBObject();
					
					toWrite = fetchUserDetails(user, collection); // Add user details
					toWrite.put("business_id", business_id); // Add business Id
					toWrite.put("Reviewed", 0); 
					toWrite.put("review_id", null);
					
					if (toWrite.toString().length() < 10) {
						System.out.println("Did not put this in the list for writing " + toWrite.toString());
						continue;
					}
					
					else outputFormat.add(toWrite.toString());
					
					/* Increment the counters */
					this.negative_count++; 
					numberOfReviews++; 
					
					if (outputFormat.size() == BUFFER_SIZE) {
						System.out.println("Output array reached buffer size. Has to flush");
						flushOutputArray();
					}
					
					if (numberOfReviews == LIMIT) break;
				}
				if (this.negative_count == NEGATIVE_NEEDED) return;
			}
			if (this.negative_count >= NEGATIVE_NEEDED) return;
		}
		
		closeAll(); // Close all connections
	}
	
	/***
	 * 
	 * This is main method to be called which internally uses above functions to write
	 * the complete classification data in JSON format
	 * 
	 * @param lim
	 * @param minCount
	 * @param maxCount
	 * @throws IOException
	 */
	public void writeClassificationData(int lim, int minCount, int maxCount) throws IOException {
		System.out.println("Getting " + lim + " businesses within given range.\n");
		getRangeReviewedBusinessesWithinRange(lim, minCount, maxCount);
		
		System.out.println(reviewedBusinesses.size());
		
		// Fill in the limit value
		// This value decides how many reviews would be selected for a business
		LIMIT = POSITIVE_NEEDED / reviewedBusinesses.size();
		
		System.out.println("Starting to fill user business hash map and output array ......");
		writePositiveTestCasesAndFillHashmap();
		
		System.out.println("Starting to write classification data file with these top businesses and filling user business hash map ......");
		
		if (this.outputFormat.size() > 1) {
			System.out.println("Final flush of output format list is required !");
			flushOutputArray();
		}
		
		System.out.println("You classification file is ready");
		
		System.out.println("Done writing reviewed entries. Now start for adding negative cases.");
		
		writeUnreviewedTestCases();
		
		System.out.println("Done writing negative test cases.");
	}
	
	public static void main(String[] args) throws IOException {
		
		// Ran with different ranges
		// 5, 2500, 4500 --> Done
		// 5, 1500, 2500 --> Done 
		// 5, 1000, 1500 --> Done
		// 5, 800, 1000 --> Done
		// 5, 600, 800 --> Done
		// 5, 400, 600 --> Done
		// 5, 200, 400 --> Done
		
		CreateFinalDataSet object = new CreateFinalDataSet();
		object.writeClassificationData(5, 1500, 4500);		
	}
}
