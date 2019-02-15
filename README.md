Crosstalk
=====================

Crosstalk is a JAVA 1.8 based interface between the campaign manager and the bidders. The campaign manager creates, edits and maintains
its campaigns in a MySQL database. Crosstalk converts those MySQL tables into a JSON representation of the campaigns as understood by
the bidders. 

Crosstalk loads the JSON representation of the campaigns into 'zerospike', which is the shared context of the bidders. Crosstalk
then tells which campaigns in the zerospike store the bidders should load.

Crosstalk also handles budgets. Querying Elastic Search, it know up to the minute what the spend on all campaigns are. If a campaign
exceeds its budget then crosstalk will tell the bidders to unload the running campaign.


No Source Deployments
===========================

Crosstalk is designed to run as a Docker cintainer.

Docker Swarm
---------------------------
Use Docker swarm to run Crosstalk, Bidder, Zerospike, Kafka and Zookeeper

1. Copy docker-compose.yml from Project's root directory.

2. Start the swarm

   $docker swarm init
   
3. Start the network

   $docker network create --driver overlay rtb_net --subnet=10.0.9.0/24
   
4. Deploy

   $docker stack deploy -c docker-compose.yml crosstalk
   
Docker Compose
-------------------------------
Use Docker Compose to run Crosstalk, Bidder, Zerospike, Kafka and Zookeeper in a single console window:

1. Copy docker-compose.yml from Project's root directory.
   
2. Start the network

   $docker network create rtb_net
   
4. Deploy

   $docker-compose up 
   
Docker Stack
--------------------------------
To run the entire crosstalk. bidder, zerospike, kafka and zookeeper as a swarm:

    $docker swarm init
    $docker network create --driver overlay rtb_net --subnet=10.0.9.0/24
    
    #
    # Join any workers, if desired; then,
    #
    
    $docker stack deploy -c docker-compose.yml crosstalk
   
Working with Source
---------------------------------
If you want to modify the code.

1. GIT clone, cd to the cloned directory.

2. Make your changes...

3. Run maven:

   $mvn assembly:assembly -DdescriptorId=jar-with-dependencies  -Dmaven.test.skip=true
   
4. Make the docker images locally (note change your repo from jacamars to your repo):

   $docker build -t jacamars/crosstalk -f Docker.crosstalk .
   
5. If you need to push to the repo:

   $docker push jacamars/crosstalk:test
   
Changing Operational Parameters
-------------------------------------
Crosstalk uses a container based file in config.json. If you need to change the parameters within it
do it in your own copy and use volumes command to mount into it. Example, suppose you made your own copy of  config.json and modified it and you called it ./myconfig.json. You modify the bidder services section in docker-compose.yml to mount. Note the volumes directive:

  crosstalk:
    image: "jacamars/crosstalk"
    ports:
      - "8200:8200"
    environment:
      REGION: "AP"
      PASSWORD: "iamspartacus"
      GHOST: "192.92.68.11"
      AHOST: "192.92.68.10"
      ESPORT: "9200"
      BROKERLIST: "kafka:9092"
      PUBSUB: "zerospike"
      CONTROL: "8100"
    volumes:
      - myconfig.json:/config.json
    networks:
      - rtb_net
    depends_on:
      - kafka
      - zerospike
    command: bash -c "./wait-for-it.sh kafka:9092 -t 120 && ./wait-for-it.sh zerospike:6000 -t 120 && sleep 1; ./crosstalk"

HANDY DOCKER ACTIVITIES
=====================================

List Running Containers
--------------------------------------

    $docker ps
    
List Images
---------------------------------------

    $docker image ls

Attach to a Running Container
---------------------------------------
Do a docker ps and then use the container-id or name:

    $docker exec -it <id-or-name> /bin/bash
    
Attach to the Log of a Running Container
---------------------------------------
Do a docker ps and then use the container-id or name:

    $docker logs -f <id-or-name>
    
Delete an Image
----------------------------------------
Do a docker ls first

    $docker image ls
    
Find the container's image id

    $docker image rm <image-id> --force
    
Correct Checksum Error
-----------------------------------------
If docker-compose complains about a checksum after you delete a container do this:

    $docker-compose rm
    
Run a container just to inspect it.
------------------------------------------
All the containers we employ use Alpine Linux. It will always have /bin/sh. Our containers also 
contain /bin/bash. To inspect the container for crosstalk do this:

    $docker image ls | grep crosstalk
    # Find the container id, let's sa it is c720081030ce
    $docker run -it c720 /bin/bash
    bash-4.3 #
    

System Overview
=====================================

*** Please Note. This is a --- Maven --- Project, not Gradle or Ant ***

This system connects to MySQL to obtain campaigns, creatives and rtb_standard rules. It connects to Elastic Search to get current spends.

1. The SQL is turned into a JSON array. The array elements are campaigns. Each campaign json object contains a json object for the target, an rtb_rules object and an array of JSON objects that represents the creatives. Each creative object has rtb_rules.

2. The first job of the system is to convert these JSON objects of SQL representations of the campaigns into RTB4FREE JSON objects. Then the system stores the RTB4FREE version into Zerospike.

3. Elastic Search is queried to update all of the current budgets total, daily and hourly. They are queried every minute.

4. Then the Bidders are told to load the active campaigns from Zerospike that have not exceeded their budgets, and are active.

Steps 1-4 are done in a loop, if any changes occur in the campaigns, the bidders are told to delete or add the associated campaign.

Other activities:

5. Bidders log their presence in Zerospike, that is timed to delete after 1 minute if not refreshed. This way if bidders crash
crosstalk can become aware of this. If a new bidder is seen, it is automatically loaded with the results of 1-4.

6. Crosstalk writes its own presence in Zerospike, that will time out in 1 minute if not refreshed. This is the deadman switch. 
If Crosstalk crashes, the deadman switch will auto delete. The bidders watch for this event and if they see this, they will stop bidding.

7. Crosstalk no longer collects  wins, bids, pixels, and clicks. These events are logged directly to ELK using kafka.

8. Crosstalk provides a web API to allow other systems to interact with the system. It is located by default on :8100/api. The kinds
of things you can do are:

a. Get current budgets.
b. Get a campaign.
c. Get prices.
d. Get reasons why campaign is not bidding.
e. Get spend rates.
f. Add/Modify weights on campaigns with rotating creatives.
g. List campaigns.
h. Ping the system.
i. Refresh the bidders with the campaigns.
j. Set budgets
k. Set prices for campaigns, creatives, deals.
l. Start bidders.
m. Stop bidders.
n. Update a  single campaign in the bidders.

Packages
-------------------------------
com.jacamars.dsp.crosstalk.api		- The web api
com.jacamars.dsp.crosstalk.budget	- Interface to Elastic Search, used for budgeting.
com.jacamars.dsp.crosstalk.config	- The configuration JSON reader
com.jacamars.dsp.crosstalk.manager	- The code that implements the crosstalk system.
com.jacamars.dsp.crosstalk.tests	- Some tests.
com.jacamars.dsp.crosstalk.tools	- Various tools 
com.jacamars.dsp.crosstalk.unified	- The unified logger

Overall Flow
-------------------------------
System's main class is in Crosstalk.java.

Crosstalk class starts a Scanner class. The scanner class periodically reads SQL and converts to JSON array. The JSON array is converted
into RTB4FREE formatted JSON and is stored in AccountingCampaign.java and AccountingCreative.java. These classes hold the RTB4FREE
representations of the campaigns. These 2 classes also make sure budgets are maintained. The Scanner loops through the AccountingCampaign
Set and then checks to see if they are online or offline, and if the budgets are within limits. If not, the AccountingCampaign is moved
to a set of parked campaigns (not deleted, because they can come back on line).

Once a minute the Budgeting is checked and the Elastic Search system is queried and all the campaigns and creatives are queried. 
A separate set of Campaigns and Creatives Maps are maintained - these are used to keep track of current spends on ALL campaigns and 
creatives. Note the AccountingCampaign and AccountingCreatives are separate.

As each AccountingCampaign first calls runUsingElk() - the spends are obtained from the BudgetControl class (via these internal 
Campaign and Creative objects). Think of it as this: AccountingCampaign and AccountingCreative maintain budget limits from the 
SQL system and the BudgetController.Campaign and BudgetController.Creative maintain current spends. The AccountingCampaign 
queries the BudgetController for the current spend.

Then the AccountingCampaign can call budgetExceeded() to see if the budget has been exceeded for total, daily, or hourly.

Each AccountingCampaign object also calls AccountingCreative.runUsingElk() to determine up to date spend amounts. Then the 
AccountingCreative.budgetExceeded() is called to see if the budget has been exceeded.

The BudgetController runs once a minute. It does Elastic Search queries for Total, Daily, and Hourly. The latency is also computed. This
latency is the lag between now, and the last time ES saw a log record for the spend. This latency is in seconds.

The spends are located in Aggregator class instances. The Aggregator has a TreeMap of Campaign objects. These Campaign objects 
are visible only within the Budget package. Each Campaign object has a list of Creatives. The Campaign and Creative objects each 
contain the spend amounts for the total, daily, hour, and the spend delta (a moving average). To find the current Campaign or creative 
spend or rate you query the BudgetController instance.

Please note, the total aggregation is the total up to 0 hour of the current day. After all the total, daily and hourly spends are 
obtained, the daily amount is added to the total to come up with the current total spend.

The lag of the Elastic Search system is known. For example, if the current lag is 2.5 minutes, that means the effective 
total spend is equal to the current spend rate (in minutes) * 2.5 + the observed total. The same calculation for effective 
spend works for hourly and daily too. This way, if we should fall behind in the log, we can still stay fairly accurate in our budgeting.

Build Crosstalk
============================
$mvn assembly:assembly -DdescriptorId=jar-with-dependencies  -Dmaven.test.skip=true

The all-in-one jar file is in ./target

Javadoc
--------------------------------
To make just the javadoc:

$mvn javadoc:javadoc

Javadoc is located in ./target/site/apidocs/index.html


Configuration Files
--------------------------------
There are 5 configuration files, 3 Elastic Search queries, a log4j.properties and the config.json.

The application configuration file is: ./config.json

The logging configuration file is:  ./log4j.properties

The Elastic Search queries  are stored in the following queries directory as: 
   ./queries/daily.json
   ./queries/total.json
   ./queries/lastlog.json

Running Crosstalk on the Local Machine
===============================================================
You can run crosstalk in development mode if you do the following.

1. First you need a bidder running, on the localhost with your crosstalk development. You can open a console window
   and use the following command to start a production zookeeper, kafka, zerospike and bidder on your local system;
   
        $docker-compose -f bidder.yml up
        
2. Now you can start your Crosstalk in a console window or  in your IDE's debugger. Note. Crosstalk 
   uses ./config.json as its default config file. You can start Crosstalk with a different configuration 
   file using the name as its argument. The region in config.json is set to NJ.
   
   Or to run in the console:
   
        $tools/crosstalk [config-file-name]
   
2. Configuration file and shell variables. The config file can access shell environment variables. Here is a 
   list of environment variables that come with defaults. If you use these and don't provide a value for the 
   shell variable, it will use the indicated default.
   
   If you use your own shell environment variables and don't provide a value, then "" will be used.
   
   Crosstalk specific:
   
   $CONTROL         "8100"
   $JDBC            "jdbc:mysql://localhost/rtb4free?user=ben&password=test"
   
   Shared with the bidder:
   
   $ADMINPORT       "8155"
   $BROKERLIST     "[localhost:9092]"
   $CONCURRENCY     "3"
   $EXTERNAL        "http://localhost:8080"
   $FREQGOV         "true"
   $HOSTNAME        Docker instance
   $INITPORT        "6002"
   $PIXEL           "localhost"
   $PUBPORT         "6000"
   $PUBSUB          "localhost"
   $REQUESTSTRATEGY "100"
   $SUBPORT         "6001"
   $THREADS         "2000"
   $VIDEO           "localhost"
   $WIN             "localhost"
   
   Note, all substitutions are strings.
   
Ping
-------------------------------------
http://ip-address-of-crosstalk:8100/status

API
-------------------------------------
http://ip-address-of-crosstalk:8100/api

POST the appropriate command

Other Directories
==================================

src			- Source code for the Java language
config		- Up to date configurations used in the all the regions
docs		- Any extraneous documentation
libs		- External libararies not maintained with Maven
logs		- Where the log files go (Not the application log, that is in /var/log/crosstalk.log
python		- Python library for interfacing with Crosstalk's api
queries		- The template queries used with Elastic Search.
shell		- Shell script/curl stuff to test and demo the web API.
target		- Where the build artifacts are stored.
tools		- Where the console tools are located

