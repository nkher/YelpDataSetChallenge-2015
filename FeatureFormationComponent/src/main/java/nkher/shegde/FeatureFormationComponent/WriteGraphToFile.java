package nkher.shegde.FeatureFormationComponent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/***
 *  Writes the graph of the user collection to a flat file
 *  in the form of adjacency lists.
 *  
 *  @author Namesh Kher, Sarika Hegde
 */

public class WriteGraphToFile {

	private DB yelpDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	private DBObject dbObject;
	private BufferedWriter bWriter;
	private File file;
	private FileWriter fWriter;
	
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
	 * 
	 * Method to write the yelp graph to a file in the form of adjacency matrices.
	 * 
	 */
	public void writeYelpGraphToFile() throws IOException {
		System.out.println("Connecting to yelp database .. ");
		setupConnection("yelp");
		System.out.println("Connecting to user collection ...");
		getCollection("complete_user");
		System.out.println("Getting users and writing to file");
		file = new File("Output/YelpGraph_All.txt");
		fWriter = new FileWriter(file);
		bWriter = new BufferedWriter(fWriter);
		dbCursor = collection.find();
		try {
			while (dbCursor.hasNext()) {
				dbObject = dbCursor.next();
				bWriter.write(dbObject.get("user_id").toString() + "\t");
				String friends = dbObject.get("friends").toString();
				friends = friends.replace("[", "");
				friends = friends.replace("]", "");
				friends = friends.replace("\"", "");
				String a_friends[] = friends.split(",");
				for (int j=0;j<a_friends.length;j++) {
					bWriter.write(a_friends[j].trim() + "\t");
				}
				bWriter.newLine();
			}
		}
		catch (Exception e) {
			System.out.println("Some exception occured while getting business ids");
		}
		finally {
			bWriter.close();
			dbCursor.close();
		}
		System.out.println("Wrote the graph to the file");
	}
	
	public static void main(String[] args) throws IOException {
		WriteGraphToFile object = new WriteGraphToFile();
		object.writeYelpGraphToFile();
	}
}
