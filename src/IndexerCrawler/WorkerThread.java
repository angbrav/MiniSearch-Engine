package cc.ist;

import java.sql.SQLException;
import java.util.StringTokenizer;

import org.htmlparser.util.ParserException;

public class WorkerThread extends Thread {

	private String[] target;
	// private Hashtable<String, LinkedList<ElementLinkedList>> table;
	private Storage st;
	private int begin, total;

	public WorkerThread(String[] target, int beg, int tot, String serverdb,
			String user, String pass) {
		super();
		this.target = target;
		this.st = new Storage(serverdb, user, pass);
		this.begin = beg;
		this.total = tot;
		// this.table = table;
	}

	public void run() {

		String text;
		StringTokenizer words;
		for (int i = this.begin; i < (this.begin + this.total); i++) {
			StringExtractor extractor = new StringExtractor(this.target[i]);
			try {
				text = extractor.extractStrings(false);
				words = new StringTokenizer(text, " \n\t\f.,;:-[]()$#/=&%!?\"·|");
				String element;
				// words = text.split(\\W, 0);
				try {
					st.connect();
					int c = 0;
					while (words.hasMoreTokens()) {
						// System.out.println(words.nextToken());
						element = words.nextToken().toUpperCase();
						if (!element.matches("IS|ARE|BE|I|AM")&&!element.matches("\\w")) {
							st.insertWordRow(element, this.target[i], c);
						}
						c++;
					}
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (ParserException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		/*
		 * if (this.table.containsKey(target)){ LinkedList<ElementLinkedList>
		 * list = this.table.get(target); list.add(new
		 * ElementLinkedList(target,5,6)); }else{ ElementLinkedList element =
		 * new ElementLinkedList(target,2,4); LinkedList<ElementLinkedList> list
		 * = new LinkedList<ElementLinkedList>(); list.add(element);
		 * this.table.put(target, list); }
		 */

	}
}
