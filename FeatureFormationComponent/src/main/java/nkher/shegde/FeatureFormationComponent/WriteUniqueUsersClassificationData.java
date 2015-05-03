package nkher.shegde.FeatureFormationComponent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/***
 * Used to create a flat file of all unique users. This is done through the following 
 * helper functions
 * 1. fillUsersSet() -> Gets all the unique users from the collections cd1 to cd7 and stores them in a Set 
 * 2. writeUsersToFile() -> Writes all the users to a file
 * 
 * Personalized Page Rank would be calculated for all these users.
 * 
 * @author nameshkher, Sarika Hegde
 */

public class WriteUniqueUsersClassificationData {
	
	private DB classificationDB;
	private MongoClient mClient;
	private DBCollection collection;
	private DBCursor dbCursor;
	private DBObject dbObject;
	private BufferedWriter bWriter;
	private File file;
	private FileWriter fWriter;
	
	public Set<String> users = new HashSet<String>();
	
	/***
	 * Creates a connection with the database
	 * 
	 * @param database name
	 * @throws UnknownHostException
	 * 
	 */
	public void setupConnection(String dbName) throws UnknownHostException {
		mClient = new MongoClient();
		classificationDB = mClient.getDB(dbName);
	}
	
	/***
	 * 
	 * Gets the collection from the specified database passed in the setupConnection() method
	 * 
	 * @param collectionName
	 */
	public void getCollection(String collectionName) {
		collection = classificationDB.getCollection(collectionName);
	}
	
	
	/***
	 * 
	 * Gets all the unique users from all the collections in the classification_data database and stores them in a Set.
	 * 
	 * @throws UnknownHostException
	 */
	public void fillUsersSet() throws UnknownHostException {
		System.out.println("Starting to add unique users to the users set ...");
		setupConnection("classification_data");
		int count = 0;
		
		Set<String> collectionSet = classificationDB.getCollectionNames();
		for (String coll : collectionSet) {
			
			if (!coll.equals("system.indexes")) {
				getCollection(coll);
				dbCursor = collection.find();
				count = 0;
				try {
					while (dbCursor.hasNext()) {
						dbObject = dbCursor.next();
						String userId = dbObject.get("user_id").toString();
						if ( users.add(userId) ) {
							count++;
						}
					}
				}
				catch (Exception e) {
					System.out.println("Some error occured !!");
				}	
				System.out.println("Added " + count + " users for collection - " + coll);
			}
		}
		
		System.out.println("Total users are : " + users.size());
	}
	
	/***
	 * 
	 * Writes all the users from the users set to a file.
	 * 
	 * @throws UnknownHostException
	 */
	public void writeUsersToFile() throws IOException {
		System.out.println("Writing users to file.");
		file = new File("Output/Users");
		fWriter = new FileWriter(file);
		bWriter = new BufferedWriter(fWriter);
		
		int count = 0;
		for (String user : users) {
			count++;
			bWriter.write(user);
			bWriter.write(",");
			if (count == 100) {
				bWriter.newLine();
				bWriter.write("----------------------------------------------------------------------------------------------------------------");
				bWriter.newLine();
				count = 0;
			}
		}
		bWriter.close();
		System.out.println("Done writing");
	}
	
	/***
	 * Closes the connection with the Mongo client
	 * 
	 */
	public void closeAll() {
		mClient.close();
		classificationDB.cleanCursors(true);
		collection = null;
	}
	
	
	public static void main(String[] args) throws IOException {
		WriteUniqueUsersClassificationData object = new WriteUniqueUsersClassificationData();
		object.fillUsersSet();
		// object.writeUsersToFile();
		object.closeAll();
	}

}
