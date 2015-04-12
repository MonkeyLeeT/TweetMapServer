TweetMapServer
==============
##About
This is the real time server to process tweets from Twitter Streaming API, pushing them to WebSockets and storing them in RDS.

Demo: Please refer to [TweetMap](https://github.com/MonkeyLeeT/TweetMap)

##Run
Complie as : javac -cp ".:../lib/\*" TwitterStream.java  
Run as : java -cp ".:../lib/\*" TwitterStream  
(Assuming that you're under /src/ folder)  
to initiate the server. It will listen on 11111 for incoming requests by default.

##Dependencies
com.google.gson : Manipulating JSON objects returned from Twitter.

com.twitter.hbc : Streaming library for Twitter.

org.java_websocket: WebSocket library.

