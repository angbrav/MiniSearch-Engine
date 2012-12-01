package cc.ist;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.htmlparser.util.ParserException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class CreatingWordsIndex {
	
	public static void main(String[] args){
		//Storage st = new Storage(args[0],args[1],args[2]);
		Storage st = new Storage(args[0],args[1],args[2]);
		Thread t=null;
	    String[] sites;
	    int max_threads = 4;
	    
	    
	    
	    
	    try {
			st.connect();
			sites = st.getSites();
			prGraphGenerator prgg;
			try {
				prgg = new prGraphGenerator(sites,st);
				try {
					prgg.fetchAlltheLinks();
					prgg.insertNewSites();
					st.close();
					String awsAccessKey = "AKIAIXDL4AVDSSMH4JJQ";
					String awsSecretKey = "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";
					AWSCredentials awsCredentials = 
					    new AWSCredentials(awsAccessKey, awsSecretKey);


					S3Service s3Service = new RestS3Service(awsCredentials);

					File graph = new File("graph.txt");
					S3Object fileObject = new S3Object(graph);

					s3Service.putObject("pagerankmam/input/iter0", fileObject);
				} catch (ParserException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			st.connect();
			sites = st.getSites();
			st.close();
			int x = (sites.length/max_threads);
			int r = (sites.length%max_threads);
			int beg=0;
			for (int i=0;i<max_threads;i++){
				if (r>0){
					t = new WorkerThread(sites,beg, x+1, args[0], args[1], args[2]);
					beg+=x+1;
					r--;
				}else{
					t = new WorkerThread(sites,beg, x, args[0], args[1], args[2]);		
					beg+=x;
				}
				t.start();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}