# Perx Data core Data Engineer Tasks

## A) SQL Test

### Prerequisite:

* Redshift (https://aws.amazon.com/redshift/free-trial/)
* Required CSV files are in folder `./data`, import into Redshift in three diffrent tables as mentioned below. (Redshift's free trial should be enough for this exercise)

* Table and Relation:
 1.  Campaign (id , name , updated_at)
 2.  Reward_campaigns( id , Reward_name , updated_at, campaign_id(FK(campaign.id)))
 3.  Reward_transactions(id, status, updated_at, Reward_camapigns_id(FK(Reward_camapigns.id)) )

### Use Case 1:

Client X would like to identify the most popular engagement day and time between a specific date range (Database stored data is in UTC format and input will be in SGT) for a Campaign.
 
   * Expected Output:
           No Of Transaction | Date | Time 

### Use Case 2:
         
Client X would like to see the weekly breakdown of reward performance along with comparison of previous week.

   * Expected Output:
          Reward name | Reward Redeemed count | Percentage difference as per previous week | Date/ Week 

### Deliverable

The part of importing data is necessary to be implemented as well instead of manually done. Using frameworks like apache spark or beam or any other big data parralel processing is recommended.

Output with SQL script for final results.

## B) Analyse HTTP Log for User Sessions

### Use Case

Client X would like to find out about the campaign performance for a certain time frame from HTTP log.

A campaign may result in http requests including:

* GET request to `/campaigns/{{id}}`
* POST request to `/rewards/{{id}}` (get a reward issued to the user)

A session window for a user is defined as: within which there's activity for a user (http request for the user) and gap between each activity is less than 5 mins.

Need to provide a report that has following fields:

* user_id
* session_start (timestamp)
* session_end (timestamp)
* campaigns: list of campaigns the user viewd
* rewards_issued: list of rewards the user got issued
* reward_driven_by_campaign_view: true if user got a reward issued after viewing a campaign that contains the reward within the session

### Tasks

1. Create an avro schema for the HTTP log and convert the http log data provided to avro format
2. Load the avro data into a Redshift table
3. Load the campaign and reward mapping csv into a redshift table
4. Do the processing to create the result

#### Data Provided

HTTP log is provided as text file with each line as one entry, it's saved in `data/http_log.txt`. Fields in the log entry are delimited by space with following format:

```
timestamp http_method http_path user_id
```

The following list describes the fields of an access log entry.

* timestamp: iso8601 timestamp (exmaple: `2019-06-01T18:00:00.829+08:00`)
* http_method
* http_path: The http request path
* user_id: User identifier

Mapping of campaign and reward is provided as a csv file in `data/campaign_reward_mapping.csv`.

Campaign and reward is many to many relationship.

### Deliverable

The avro schema needs to be created manually as avsc file. All other tasks need to be encapsulated in a framework project and shared via a private github repo.

Within framework project, you are free to use any python library.

Even though the provided dataset is small, writing code that can accommodate very large dataset is preferred. Using frameworks like apache spark or beam or any other big data parralel processing is recommended.


# Perx Data :: Visualization and Machine Learning

## B) Data Analysis and visualisation Task

### Use Case

Client X would like to see the BI Dashboard to analyses user engagement and Rewards insights.

### Specification

 Create a BI Dashboard to analyse User engagement and Reward insights 

### Data Provided

Please use the file /data/interview_test_voucher_Data.csv that has reward detailed and the associated voucher details. Its also has user details who received voucher.

### Deliverable

A working BI Dashboard that show User engagement Insights and reward performance/engagements.

## B) Machine Learning for Recommendation Task

### Use Case

Client X would like to understand its customers behaviour to recommend relevant rewards for future campaigns.

### Specification

Create a working machine learning (unsupervised model) solution that recommend rewards when input a user_id.

You are free to use any Python framework including tensorflow, Keras, scikit-learn and pytorch.

### Data Provided

Please use the public [movielens 1M dataset](https://grouplens.org/datasets/movielens/1m)

### Deliverable

A command line application that can be invoked with user_id as parameter and output recommended movie ids. Or a simple REST service that is packaged as docker image that expose a simple rest API that returns recommended movie ids for a user specified.

Bonus point: recommended to have a orchestration application, either localhost or docker, to help update the data as well as retrain the model and finally update to the latest model in order to support the recommendation machenism within, which links to the core application itself. Draft CI/CD architecture for this application then.
