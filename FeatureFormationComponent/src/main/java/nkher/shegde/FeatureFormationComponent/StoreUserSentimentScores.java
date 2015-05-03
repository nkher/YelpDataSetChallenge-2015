package nkher.shegde.FeatureFormationComponent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

/***
 * Creates a hashmap of all businesses and users who reviewed it with other details.
 * The fillUserSentimentsHashMap() function takes in a file of a specified format to do this.
 *  
 *  @author Namesh Kher, Sarika Hegde
 */

public class StoreUserSentimentScores {
	
	private BufferedReader bReader;
	private File file;
	private FileReader fReader;
	
	
	public HashMap<String, HashMap<String, ArrayList<SentimentDetails>>> userSentiments = new HashMap<String, HashMap<String, ArrayList<SentimentDetails>>>();
	
	/***
	 * 
	 * Takes the path of a file (csv or text) and file name containing user reviews in the following format
	 * {id_no, userId, reviewId, review_count, review_date, review_text, businessId, review_sentiment_score}
	 * Creates a hashmap of business Ids with value as another hashmap of all the users who have reviewed 
	 * for that business. The inner hashmaps value is a list of SentimentDetails objects. This contains the
	 * Sentiment Score, Sentiment Date and the review Id for that review. This is stored as an array list as
	 * one user could review many times for a business.
	 * 
	 * @param filename
	 * @param path
	 * @throws FileNotFoundException
	 */
	public void fillUserSentimentsHashMap(String filename, String path) throws FileNotFoundException {
		System.out.println("Filling the user sentiments hashmap by business");
		
		String complete = path + "/" + filename;
		file = new File(complete);
		fReader = new FileReader(file);
		bReader = new BufferedReader(fReader);
		int lines = 0;
		int count = 0;
		String line = "";
		try {
			while ( (line = bReader.readLine()) != null ) {
				
				if (count == 0) {
					count++;
					continue;
				}
				
				// Format of file
				// 1,LWbYpcangjBMm4KPxZGOKg,6w6gMZ3iBLGcUM4RBIuifQ,5,12/1/12,review,mVHrayjG3uZ_RLHkLj-AMg,13

				
				line = line.replace("\"", "");
				String arr[] = line.split("\\s+");
				String businessId =  arr[6].trim();
				String reviewId = arr[2].trim();
				String userId = arr[1].trim();
				int score = Integer.parseInt(arr[7].trim());
				String date = arr[4].trim();
				
				// System.out.println("UId : " + arr[2] + " ,rid : " + arr[3] + " bid : " + arr[7] + " , score :" + arr[8] + " date " + arr[5]);
								
				if (!userSentiments.containsKey(businessId)) { // If business is not in the hashmap
					HashMap<String, ArrayList<SentimentDetails>> allUsers = new HashMap<String, ArrayList<SentimentDetails>>();
					
					SentimentDetails sentiDetails = new SentimentDetails(score, date, reviewId);
					
					ArrayList<SentimentDetails> listOfSentiments = new ArrayList<SentimentDetails>();
					listOfSentiments.add(sentiDetails); // Adding the review to the users sentiment list
					
					allUsers.put(userId, listOfSentiments); // add the user id and his list of sentiments to inner hashmap
					
					userSentiments.put(businessId, allUsers); // add it to final outer hashmap
				}
				else {
					HashMap<String, ArrayList<SentimentDetails>> allUsers = userSentiments.get(businessId);
					
					SentimentDetails newDetail = new SentimentDetails(score, date, reviewId);
					ArrayList<SentimentDetails> listOfSentiments;
					
					if (allUsers.containsKey(userId)) {	// Check if a this review is a second or more review from the that user
						listOfSentiments = allUsers.get(userId);
					}
					else {
						listOfSentiments = new ArrayList<SentimentDetails>();
					}
					listOfSentiments.add(newDetail);
					allUsers.put(userId, listOfSentiments);
					userSentiments.put(businessId, allUsers);

				}
				lines++;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("Some error occured while reading the sentiment user flat file");
		}
		
		System.out.println("Lines read : " + lines);
		System.out.println("Done filling hashmap.");
	}
	
	
	
	public static void main(String[] args) throws FileNotFoundException {
		
		StoreUserSentimentScores object = new StoreUserSentimentScores();
		object.fillUserSentimentsHashMap("sentiment_file.txt", "/Users/nameshkher/Documents/Semester_IV/Big_Data_Insights");
		
		System.out.println("Total businesses : " + object.userSentiments.size());
		
		
		// System.out.println(object.userSentiments.get("PXviRcHR1mqdH4vRc2LEAQ"));
	}
}

/***
 * Data Structure to hold Sentiment Details in it.
 * Includes Sentiments review id, Review's sentiment Score and reviews date.
 * 
 *  @author Namesh Kher, Sarika Hegde
 */

class SentimentDetails {
	
	public int sentimentScore;
	public String sentimentDate;
	public String sentimentReviewId;
	
	public SentimentDetails(int score, String date, String reviewId) {
		sentimentScore = score;
		sentimentDate = date;
		sentimentReviewId = reviewId;
	}
	
	public String toString() {
		return "[ review_id : " + sentimentReviewId + ", score : " + sentimentScore + ", date : " + sentimentDate + " ]";
	}
}
