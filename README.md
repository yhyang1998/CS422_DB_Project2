# CS422 Project 2 - Report

## Task 1: Bulk-load data

- TitlesLoader: Read the **title** RDD data from the file using the spark context and then translate it to specific type using ```pattern matching```
- RatingsLoader: Read the **rating** RDD data from the file using the spark context and then translate it to specific type using ```pattern matching``` (The tricky thing is that we need to deal with the old matching, which can be *None*)

## Task 2: Average rating pipeline
- Aggregator
	- init(): In the very beginning, I need to turn the received RDD to PairRDD (for both **title RDD** and **rating RDD**) using a map function so that I am able to do further calcution. Then, I do an aggregattion respectively on the sum and number of each title_id using ```aggregateByKey```. Later, I create a avg_rating_map which is a map with key being the title_id and value being the sum and number of each tid. In the end, I create a ```title_map``` which store the original (tid, (tname, avg_rating))
	- getResult(): return the ```title_map``` in a specific format. (Here, since the ```title_map``` is a global variable, it will record the update)
	- getKeywordQueryResult(): First, I create a ```filtered_title_map``` which filter out all the keywords that are not in the ```title_map```. Then, the rating is calculated by joining the ```filtered_title_map``` and the ```avg_rating_map```. (Here, since the ```avg_rating_map``` is a global variable, it will record the update)
	- updateResult(): It is the most tricky part. I create ```upd_rating_sum, upd_rating_num, upd_rating_avg``` to store the update result. Then do a fullOuterJoin with ```avg_rating_map``` so that I can update the avg rating properly. After that, I need to update the ```title_rating``` by leftouterjoin it with ```avg_rating_map```. The reason of using the leftouterjoin instead of a fullOuterJoin is because that we only care about the title that is in the ```title_map```
	
## Task 3: Similarity-search pipeline
- LSHIndex:
	- hash_per_row(): It's a function that do hash on each row of RDD(instead of hashing the whole RDD)
	- hash(): Do the hashing
	- getBuckets(): Need to put together the signature and the titles' keywords. I do this by a join.
	- lookup(): Try to retrieve the titles that matches the same signature. I do this by leftOuterJoin the buckets on queries
- NNLookup: Basically connect the API of LSHIndex
- NNLookupWithCache()
	- We need to build a cache which is a Map that  store the signatures and its corresponded titles. Then we also need a histogram to keep track with the cache miss/hit.
	- cacheLookup(): Take the queries -> check the cache and if it's a hit, put the result in the first rdd, if it's not return the RDD of (signature, keyword list) pairs to lookup fo the normal lookup
	- loopup(): perform the normal lookup