package cc.ist;

import org.htmlparser.filters.*;
import org.htmlparser.util.*;
import org.htmlparser.tags.*;
import org.htmlparser.*;
import java.util.*;

import java.io.*;

public class prGraphGenerator {

	List<String> inputPages = new ArrayList<String>();
	Storage st;
	int i1, i2;

	public prGraphGenerator(String[] sites, Storage storage) throws IOException {
		i2=sites.length;
		for (int i = 0; i < i2; i++) {
			inputPages.add(sites[i]);
		}
		st = storage;
		i1=0;
		

	}

	// Mudit - Fetching all the links level by level
	public void fetchAlltheLinks() throws ParserException, Exception {
		BufferedWriter out_graphTXT = new BufferedWriter(new FileWriter(
				"graph.txt"));
		int index = 0;
		int level = 0;
		int i1_temp, i2_temp;
		int limit = 3;
		//int x=0;

		// going over each Web site and fetching the links from there
		while (i1 < i2 && level < limit) {
			i1_temp = i1;
			i2_temp = i2;
			level++;
			for (int i = i1_temp; i < i2_temp; i++) {
				Parser htmlParser = null;
				try {
					htmlParser = new Parser(inputPages.get(i));
				} catch (Exception e) {

				}
				// printing the index for page
				System.out.println(inputPages.get(i));
				out_graphTXT.write((Integer.toString(i + 1)));
				try {
					final NodeList tagNodeList = htmlParser
							.extractAllNodesThatMatch(new NodeClassFilter(
									LinkTag.class));

					// printing the initial page rank, previous page rank, and
					// degree
					out_graphTXT.write(" 1 1 " + tagNodeList.size());

					for (int j = 0; j < tagNodeList.size(); j++) {

						final LinkTag loopLink = (LinkTag) tagNodeList
								.elementAt(j);
						final String loopLinkStr = loopLink.getLink();
						if (loopLink.getLink().startsWith("http")) {
							// System.out.println((x++)+" "+loopLinkStr+" Level "+level);
							int idx = inputPages.indexOf(loopLinkStr);
							if (idx != -1) {
								out_graphTXT.write(" " + (idx + 1));
							} else {
								inputPages.add(loopLinkStr);
								i2++;
								idx = inputPages.indexOf(loopLinkStr);
								out_graphTXT.write(" " + (idx + 1));
							}

						}
					}
				} catch (Exception e) {
					// e.printStackTrace(); // TODO handle error
					out_graphTXT.write(" 1 1 0");
				} finally {
					index++;
					out_graphTXT.newLine();
				}
			}
			i1 = i2_temp;
		}
		while (i1 < inputPages.size()) {
			out_graphTXT.write(Integer.toString(i1 + 1) + " 1 1 0");
			out_graphTXT.newLine();
			i1++;
		}
		out_graphTXT.close();
	}

	// Mudit - New function to insert the newly found websites
	public void insertNewSites() {
		int id = 1;
		try {
			for (id = 1; id < inputPages.size(); id++) {
				if (!st.isRepeated(inputPages.get(id))) {
					st.insertSite(inputPages.get(id));
				}
			}
			st.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}