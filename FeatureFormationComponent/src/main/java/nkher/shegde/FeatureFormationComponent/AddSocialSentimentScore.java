package nkher.shegde.FeatureFormationComponent;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


/***
 * 
 * Adds the the social sentiment score for a user, business combination to the final dataset. 
 * Logic is explained in the addSocialSentimentScore() function.
 * 
 * @author nameshkher, Sarika Hegde
 * 
 */
public class AddSocialSentimentScore {

	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	// private SimpleDateFormat dateFormat_CSV = new SimpleDateFormat("M/dd/yy");
	private SimpleDateFormat dateFormat_Mongo = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat dateFormat_CSV = new SimpleDateFormat("yyyy-MM-dd");

	
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
	 * Adds a field to the data set named 'Social Sentiment Score' with a calculated numeric value.  
	 * It checks if a users friend has reviewed for that business and considers that value if yes, else 0.
	 * Takes an average measure of all the friends who have reviewed for that business. The final numeric value 
	 * is a measure of how a users network influences him. It could be a negative value (negative influence) or positive
	 * value (positive value) or Zero indicating no influence.
	 * 
	 * @param userSentiments
	 * @throws UnknownHostException
	 */
	public void addSocialSentimentScore(HashMap<String, HashMap<String, ArrayList<SentimentDetails>>> userSentiments) throws UnknownHostException {
		setupConnection("classification_data");
		getCollection("cd7");
		
		System.out.println("Starting to add the Social Sentiment Score to all the rows.");
		
		HashMap<String, ArrayList<SentimentDetails>> allUserReviews;
		Date userDate, friendDate;
		double socialSentimentScore = 0, numberOfFriends = 0;
		
		dbCursor = collection.find();
		try {
			while (dbCursor.hasNext()) {
				DBObject row = dbCursor.next();
				String businessId = row.get("business_id").toString();
				String userReviewDate = row.get("reviewed_date").toString().trim();
				// String userId = row.get("user_id").toString();
				@SuppressWarnings("unused")
				String reviewId = "";
				if (row.get("review_id") != null) 
					reviewId = row.get("review_id").toString();
				else reviewId = "null";
				
				allUserReviews = userSentiments.get(businessId);
								
				@SuppressWarnings("unchecked")
				ArrayList<String> friends = (ArrayList<String>) row.get("friends");
				
				if (!userReviewDate.equals("NA")) {
					userDate = dateFormat_Mongo.parse(userReviewDate);
					for (String friend : friends) {
						// Check if friend has reviewed before for that business, if yes gets his sentiment score
						if (allUserReviews.containsKey(friend)) {
							ArrayList<SentimentDetails> friendReviewList = allUserReviews.get(friend);
							for (SentimentDetails sd : friendReviewList) {
								friendDate = dateFormat_CSV.parse(sd.sentimentDate); // friends date
								
								if (friendDate.compareTo(userDate) < 0) {
									socialSentimentScore += sd.sentimentScore; // add the friends sentiment score
									numberOfFriends++;
								}
							}
						}
					}
				}
				else {
					socialSentimentScore = 0; numberOfFriends = 0;
				}
			
				if (socialSentimentScore != 0 || numberOfFriends != 0) socialSentimentScore /= numberOfFriends;				
				DBObject social_Sentiment_Score = new BasicDBObject().append("$set", new BasicDBObject("Social Sentiment Score", socialSentimentScore));
				collection.update(row, social_Sentiment_Score);
				socialSentimentScore = 0; numberOfFriends = 0;				
			}
		}
		catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.out.println("Some exception occured while accessing the review JSON file");
		}
		
		System.out.println("Added the Social Sentiment Score to all the rows.");
	}
	
	
	public static void main(String[] args) throws ParseException, FileNotFoundException, UnknownHostException {
		
		
		StoreUserSentimentScores scoresObject = new StoreUserSentimentScores();
		scoresObject.fillUserSentimentsHashMap("sentiment_file.txt", "/Users/nameshkher/Documents/Semester_IV/Big_Data_Insights");
		
		System.out.println("Size of hashmap : " + scoresObject.userSentiments.size());
		
		AddSocialSentimentScore object = new AddSocialSentimentScore();
		
		object.addSocialSentimentScore(scoresObject.userSentiments);
		
//		
//		String d1 = "2012-12-01";
//		String d2 = "2014-06-27";
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//		
//		Date date1 = new Date();
//		date1 = dateFormat.parse(d1);
//		
//		Date date2 = new Date();
//		date2 = dateFormat.parse(d2);
//		
//		if (date1.compareTo(date2) < 0) {
//			System.out.println("Date 1 is smaller");
//		}
//		else
//			System.out.println("Date 2 is smaller");
	}
}
