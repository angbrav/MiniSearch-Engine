package cc.ist;

import java.io.*;

import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class RankingtoDB {

	public static void main(String[] args) {

		Storage st = new Storage(args[0], args[1],args[2]);

		try {
			String awsAccessKey = "AKIAIXDL4AVDSSMH4JJQ";
			String awsSecretKey = "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";
			AWSCredentials awsCredentials = 
			    new AWSCredentials(awsAccessKey, awsSecretKey);


			S3Service s3Service = new RestS3Service(awsCredentials);
			S3Object list[] = s3Service.listObjects("pagerankmam");
			for (int i=0; i< list.length; i++){
				if (list[i].getName().startsWith("input/output/")){
					// Open the file that is the first
					// command line parameter
					
					//System.out.println(list[i].getName());
					S3Object objectComplete = s3Service.getObject("pagerankmam/input/output", list[i].getName().split("/")[2]);
					//System.out.println("S3Object, complete: " + objectComplete);

					// Read the data from the object's DataInputStream using a loop, and print it out.
					//System.out.println("Greeting:");
					BufferedReader reader = new BufferedReader(
					    new InputStreamReader(objectComplete.getDataInputStream()));
					String data = null;
					st.connect();
					while ((data = reader.readLine()) != null) {
						String[] results = data.split("\t");
						st.insertPageRankRow(Integer.parseInt(results[0]), Double.parseDouble(results[1].split(" ")[0]));
						//System.out.println(Integer.parseInt(results[0])+" - "+Double.parseDouble(results[1].split(" ")[0]));
					    //System.out.println(data);
					}
					st.close();			
				}
			}
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

	}

}