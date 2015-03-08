import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStream {
    static Properties twitterKey;
    static TweetServer tweetServer;
    static int port = 11111;

    public static void main(String[] args) {
        twitterKey = new Properties();
        try {
            tweetServer = new TweetServer(port);
            tweetServer.start();
            System.out.println( "TweetServer started on port: " + tweetServer.getPort());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            twitterKey.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("credentials.ini"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
        Thread t = new QueueThread(msgQueue);
        t.start();


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("food", "game", "sport", "music");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(twitterKey.getProperty("consumerKey"), twitterKey.getProperty("consumerSecret"), twitterKey.getProperty("token"), twitterKey.getProperty("tokenSecret"));
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        hosebirdClient.connect();
    }

    public static class QueueThread extends Thread {
        BlockingQueue<String> msgQueue;

        public QueueThread(BlockingQueue<String> msgQueue) {
            super();
            this.msgQueue = msgQueue;
        }

        public void run() {
            Gson gson = new Gson();
    		Rds rds = Rds.getInstance();
    		if (!rds.isPasswordSet())
    			rds.setPassword(readPass());
            while (true) {
                try {
                    String msg = msgQueue.take();
                    Tweet tweet = gson.fromJson(msg, Tweet.class);
                    if (tweet.coordinates != null) {
                    	rds.insert(scan(tweet.text), tweet);
                        tweetServer.publish(gson.toJson(tweet));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
    	@SuppressWarnings("resource")
		private String readPass() {
    		InputStream password = Thread.currentThread().getContextClassLoader().getResourceAsStream("credentials.ini");
            String pass = null;
            pass = new Scanner(password).next();
            return pass;
    	}
        
        private boolean[] scan(String msg) {
        	boolean[] res = {false, false, false, false};
        	res[0] = msg.toLowerCase().contains("food");
        	res[1] = msg.toLowerCase().contains("game");
        	res[2] = msg.toLowerCase().contains("music");
        	res[3] = msg.toLowerCase().contains("sport");
        	return res;
        }
    }

}