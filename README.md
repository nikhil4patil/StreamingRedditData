# Part 1: How to run on Docker

Heads up: you will need to logout or restart after downloading Docker Desktop if you do not have it installed already.

1) Download [Docker Desktop](https://www.docker.com/products/docker-desktop/) 
2) Download and extract **StreamingRedditData.zip** file submitted on eLearning
3) Go to the **StreamingRedditData** directory in command prompt or terminal
	- Should see docker-compose file, spark, reddit-api, and logstash dirs
4) Run `docker compose up --build`
	- It takes a while to download and execute the first time, ~15 mins
	- Should start seeing logs like this
	<img width="846" alt="Screenshot 2024-04-12 at 1 42 35 AM" src="https://github.com/nikhil4patil/StreamingRedditData/assets/23207859/9d7271d9-334c-412b-bd36-7aaff95301ca">
5) Go to [localhost:5601](localhost:5601) to access the Kibana UI
	1) Click on "**+ Add Integrations**"
	2) On top, Search Elastic for "**Index Patterns**"
	3) Select "**Kibana / Index Patterns**" from the search results
	4) "**+ Create index pattern**"
		1) Name: "**entities\***"
		2) Timestamp Field: **@timestamp**
		3) Click on "**Create index pattern**"
6) Create a dashboard visualization with the frequent entities
	1) Click on the burger/menu option in top left (near the green D)
	2) Under "**Analytics**", choose "**Dashboard**"
	3) "**Create visualization**"
	4) Ensure **entities\*** is selected on the menu on the right side
	5) Choose "**Bar Horizontal**" instead of "Bar Vertical Stacked"
	6) "**Horizontal axis**"
		1) Field: **entity.keyword**
		2) Number of values: **10**
		3) **Advanced** -> Toggle off "**Group other values as Other**"
		4) **Close** (at bottom right)
	7) "**Vertical axis**"
		1) Function: **Maximum**
		2) Field: **count**

![Steps](https://github.com/nikhil4patil/StreamingRedditData/assets/23207859/d485e14e-c616-45e4-9eb9-0aaabce36633)

# Part 2: Run on Colab
