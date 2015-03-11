import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Rds {
    final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    final String DB_URL = "jdbc:mysql://tweet.cssmopf7grit.us-east-1.rds.amazonaws.com/tweets";
    private String password = null;
    private static HashMap<String, String> map = new HashMap<String, String>();
    private static String[] table = {"food", "game", "music", "sport"};
    
    private Connection conn;
    private static Rds instance = null;
    
    private Rds() {
    	conn = null;
    }
    
    public static Rds getInstance() {
    	if (instance == null)
    		instance = new Rds();
    	return instance;
    }
    
    public boolean isConnected() {
    	return conn != null;
    }
    
    public void setPassword(String password) {
    	this.password = password;
    	if (conn == null)
    		init();
    }
    
    public boolean isPasswordSet() {
    	return this.password != null;
    }
    
    public void init() {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, "FallMonkey", password);
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    public List<SelectResult> select(String table, String start, String end) {
        String sql = "SELECT * FROM " + table + 
        		" WHERE created_at < '" + end + 
        		"' AND created_at > '" + start +
        		"'";
        Statement stmt;
        int count = 0;
        List<SelectResult> list = new LinkedList<SelectResult>();
        while (true) {
            try {
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()){
                    SelectResult sr = new SelectResult(rs.getString("id_str"));
                    sr.setText(rs.getString("text"));
                    sr.setCoor1(rs.getString("longitude"));
                    sr.setCoor2(rs.getString("latitude"));
                    sr.setTime(rs.getString("created_at"));
                    list.add(sr);
                    count++;
                }
                rs.close();
                stmt.close();
                conn.close();
                break;
            } catch (Exception e) {
            	System.out.println("Reconnect to database in 3 seconds.");
				try {
					Thread.currentThread().sleep(3000);
					conn.close();
				} catch (Exception e1) {
					e1.printStackTrace(System.out);
				}
            	init();
                e.printStackTrace(System.out);
            }
        }
        System.out.println("Total count of tweets:" + count);
        return list;
    }
    
    public void insert(boolean[] mask, Tweet tweet) throws InterruptedException {
        Statement stmt;
        while (true) {
        	try {
        		stmt = conn.createStatement();
        		Tweet.Coordinate coor = tweet.coordinates;
        		String longtitude = coor.coordinates.get(0).toString();
        		String latitude = coor.coordinates.get(1).toString();
        		String timestamp = convertTime(tweet.created_at);
        		String text = tweet.text.replaceAll("\u0027", "'\'");
        		
        		for (int i = 0; i < mask.length; i++) {
        			if (mask[i]) {
        				String sql = "INSERT INTO " + table[i] + " VALUES ('" 
        						+ tweet.id_str + "', '" 
        						+ timestamp + "', '" 
        						+ text + "', '" 
        						+ longtitude + "', '"
        						+ latitude + "')";
        				stmt.executeUpdate(sql);
        			}
        		}
        		stmt.close();
                conn.close();
        		break;
			} catch (Exception e) {
				System.out.println("Reconnect to database in 3 seconds.");
				try {
					Thread.currentThread().sleep(3000);
					conn.close();
				} catch (Exception e1) {
					e1.printStackTrace(System.out);
				}
            	init();
                e.printStackTrace(System.out);
        	}
        }

    }
        
        private String convertTime(String date) {
            String processed = null;
            if(map.size() == 0){
                createMap();
            }
            String[] s = date.split(" ");
            String year = s[5];
            String month = s[1];
            String day = s[2];
            String time = s[3];
            processed = year + "-" + map.get(month) + "-" + day + " " + time;
            Timestamp timestamp = Timestamp.valueOf(processed);
            return String.valueOf(timestamp.getTime());
        }
       
        private void createMap() {
            map.put("Jan", "01");
            map.put("Feb", "02");
            map.put("Mar", "03");
            map.put("Apr", "04");
            map.put("May", "05");
            map.put("Jun", "06");
            map.put("Jul", "07");
            map.put("Aug", "08");
            map.put("Sep", "09");
            map.put("Oct", "10");
            map.put("Nov", "11");
            map.put("Dec", "12");
        }
}