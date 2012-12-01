package cc.ist;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Storage {
	private String url;
	private String pass;
	private String user;
	private Connection con = null;

	public Storage(String url, String pass, String user) {
		this.setUrl("jdbc:mysql://"+url);
		this.setPass(pass);
		this.setUser(user);
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getUser() {
		return user;
	}

	public void connect() throws SQLException {
		try {
			con = DriverManager.getConnection(this.url, this.user, this.pass);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean insertWordRow(String word, String site, int position) {
		PreparedStatement st;
		try {
			st = this.con.prepareStatement("INSERT INTO hashTable VALUES(NULL,?,?,?)");
			st.setString(1, word);
			st.setInt(2, this.getSiteId(site));
			st.setInt(3, position);
			st.executeUpdate();
			st.close();
			return true;
		} catch (SQLException e) {
			//System.out.println(word);
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}
	}
	public boolean isRepeated(String url){
		Statement st;
		boolean repeated = false;
		try {
			st = con.createStatement();
			st.executeQuery("SELECT id FROM sites WHERE name='" + url + "'");
			ResultSet rs = st.getResultSet();
			while (rs.next()) {
				repeated=true;
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return repeated;
	}
	
	// Mudit - Added a method to insert newly found sites 
	public boolean insertSite(String url) {
	      PreparedStatement st;
		try {
			st = this.con.prepareStatement("INSERT INTO sites VALUES(NULL,?,1)");
			st.setString(1,url);
			st.executeUpdate();
			st.close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	public int getSiteId(String Site) {
		int id = 9999;
		Statement st;
		try {
			st = con.createStatement();
			st.executeQuery("SELECT id FROM sites WHERE name='" + Site + "'");
			ResultSet rs = st.getResultSet();
			while (rs.next()) {
				id = rs.getInt("id");
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	
	public String[] getSites() {
		int total=0;
		Statement st;
		ResultSet rs;
		try {
			st = con.createStatement();
			st.executeQuery("SELECT COUNT(id) AS total FROM sites");
			rs = st.getResultSet();
			while (rs.next()) {
				total = rs.getInt("total");
			}
			rs.close();
			st.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String[] sites= new String[total];
		int i=0;
		try {
			st = con.createStatement();
			st.executeQuery("SELECT name FROM sites");
			rs = st.getResultSet();
			while (rs.next()) {
				sites[i] = rs.getString("name");
				i++;
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sites;
	}
	public boolean insertPageRankRow(int id, double pr) {
		PreparedStatement st;
		try {
			st = this.con
					.prepareStatement("UPDATE sites SET pagerank = ? WHERE id = ?");
			st.setDouble(1, pr);
			st.setInt(2, id);
			st.executeUpdate();
			st.close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

}
