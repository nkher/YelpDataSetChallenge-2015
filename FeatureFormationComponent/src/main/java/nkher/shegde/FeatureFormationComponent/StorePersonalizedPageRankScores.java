package nkher.shegde.FeatureFormationComponent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class StorePersonalizedPageRankScores {

	private BufferedWriter bWriter;
	private File file;
	private FileReader fReader;
	private BufferedReader bReader;
	private FileWriter fWriter;
	
	private HashMap<String, Double> userPageRanks = new HashMap<String, Double>();
	
	public HashMap<String, Double> getUserPageRanksHashMap() {
		return userPageRanks;
	}
	
	public void readPageRanks(String readPath, String readFileName) throws IOException {
		String completeIn = readPath + "/" + readFileName;
		file = new File(completeIn);
		fReader = new FileReader(file);
		bReader = new BufferedReader(fReader);
		
		int faulty = 0, lines = 0;
		String arr[], prev = null, curr;
		double pageRank = 0.0;
		String line = "";
		try {
			while ( (line = bReader.readLine()) != null) {
				lines++;
				if (line.equals("")) {
					continue;
				}
				if (line.contains("Source:")) {
					arr = line.split(" ");
					prev = arr[1];
				}
				else {
					arr = line.split(" ");
					curr = arr[1];
					if (curr.equals(prev)) {
						pageRank =  Double.parseDouble(arr[0]);
						if (!userPageRanks.containsKey(curr)) 
							userPageRanks.put(curr, pageRank);
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Some error occurred while reading the user personalized page ranks file.");
		}
		finally {
			bReader.close();
		}
		
		System.out.println("Dups count : " + faulty + " Lines: " + lines ) ;
		System.out.println("Stored " + userPageRanks.size() + " users and their personalized page ranks in the hashmap.");
	}
	
	public void writePageRanks(String writePath, String writeFileName) throws IOException {
		String completeOut = writePath + "/" + writeFileName;
		file = new File(completeOut);
		fWriter = new FileWriter(file);
		bWriter = new BufferedWriter(fWriter);
		
		/* Adding the column header */
		bWriter.write("User,Personalized Page Rank");
		bWriter.newLine();

		try {
			for (String user : userPageRanks.keySet()) {
				bWriter.write(user);
				bWriter.write(",");
				bWriter.write(userPageRanks.get(user).toString());
				bWriter.newLine();
			}
		}
		catch(Exception e) {
			System.out.println("Some error occured while writing the users and their personlized page ranks to csv");
		}
		
		finally {
			bWriter.close();
		}
		System.out.println("Wrote " + userPageRanks.size() + " users and their personalized page ranks to the csv file.");
	}
	
	public static void main(String[] args) throws IOException {
		
		StorePersonalizedPageRankScores object = new StorePersonalizedPageRankScores();
		String readPath = "/Users/nameshkher/Documents/Semester_IV/Big_Data_Insights", readFileName = "Personalized_PR_AllUsers";
		object.readPageRanks(readPath, readFileName);	
		
		// System.out.println(object.userPageRanks.get("dIrqTgqcBVQeQB6N7yzufw")); 
		// System.out.println(object.userPageRanks.get("S5zDAjR2JDPU6ZLWwVTJ3A")); 
		// System.out.println(object.userPageRanks.get("qdRnob528bRfEhHFbAXI6Q"));
	}
}
