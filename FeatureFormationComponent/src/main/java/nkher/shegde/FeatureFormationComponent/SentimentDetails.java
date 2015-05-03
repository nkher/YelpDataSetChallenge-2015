package nkher.shegde.FeatureFormationComponent;

/***
 * Data Structure to hold Sentiment Details in it.
 * Includes Sentiments review id, Review's sentiment Score and reviews date.
 * 
 *  @author Namesh Kher, Sarika Hegde
 */
public class SentimentDetails {
		
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
