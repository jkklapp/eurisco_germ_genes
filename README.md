This repo contains code to work with germplasm data from GBIF and genetic data from genebanks.

gbif_genebank_mongo.sh:
	
	This script chains 3 mapreduce tasks to retrieve germplasm occurrences from GBIF, link the occurrences with gene data from genebanks and store the result documents into a mongoDB instance. It assumes you have a Cloudera Hadoop instance deployed on localhost along with a MongoDB database.

	usage: ./gbif_genebank_mongo.sh <input_file_on_dfs> <target_mongo_database>
