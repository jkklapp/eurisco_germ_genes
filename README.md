eurisco_germ_genes
==============

*This repo contains code to work with germplasm data from GBIF and genetic data from genebanks.*

gbif_genebank_mongo.sh
--------------
	
- This script chains 3 mapreduce tasks to retrieve germplasm occurrences from GBIF, link the occurrences with gene data from genebanks and store the result documents into a mongoDB instance. It assumes you have a Cloudera Hadoop instance deployed on localhost along with a MongoDB database. The input file is a file containing numbers from 1..N per line. This is the easiest way to tell how many data from GBIF you want to retrieve. I have made tests with N=100, N=1000 and N=10000.

	./gbif_genebank_mongo.sh <input_file_on_dfs> <target_mongo_database>

store_gbif_mongo.sh
-------------

- This script retrieves data from Eurisco GBIF dataset and stores documents into specified mongo DB instance. The input file is a file containing numbers from 1..N per line. This is the easiest way to tell how many data from GBIF you want to retrieve. I have made tests with N=100, N=1000 and N=10000.

	./store_gbif_mongo.sh <input_file_on_dfs> <target_mongo_database>
