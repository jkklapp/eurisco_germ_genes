#!/bin/bash
####################################################################
####### Jaakko Lappalainen. jkk.lapp@gmail.com - 2014 #############
####################################################################
if [ $# != 2 ];then
	echo "usage: store_gbif_dataset.sh <input_file> <target-mongo-db>"
	exit 1
fi

####### PARAMETERS
date=`date +%s`
DB=$2
input=$1
# Mappers and reducers' templates
getGBIFData='#!/usr/bin/env python\n\rimport urllib2\n\rimport json\n\rimport sys\n\rbase = "http://api.gbif.org/v0.9/occurrence/search?datasetKey=865df020-f762-11e1-a439-00145eb45e9a&limit=20&offset="\n\r\n\rfor d in sys.stdin:\n\r\tresponse = urllib2.urlopen(base+str(int(d)*20))\n\r\tcontent = response.read()\n\r\tdoc = json.loads(content)\n\r\tfor r in doc["results"]:\n\r\t\ttry:\n\r\t\t\tprint "%s|%s" % (r["scientificName"],r)\n\r\t\texcept:\n\r\t\t\tpass\n'
storeInMongo="#!/usr/bin/env python\n\rimport sys,json\n\rfrom pymongo import MongoClient\n\r\n\rclient = MongoClient('ie3')\n\rdb = client."$DB"\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tsp,data = line.split('|',1)\n\r\tcol = db[sp.replace('.','')]\n\r\ttry:\n\r\t\tdoc=json.loads(data.replace(\"u'\",'\"').replace(\"'\",'\"').replace('XXXX',''))\n\r\texcept:\n\r\t\tcontinue\n\r\tcol.insert(doc)\n"
mrstream="/home/hduser/yarn/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar"

#echo -e $getGBIFData > getGBIFData.py
#echo -e $getFullRecFromIds > getFullRecFromIds.py
#echo -e $getNucIdsMap > getNucIdsMap.py
#echo -e $storeInMongo > storeInMongo.py
#echo -e $justPrint > justPrint.py
#exit

#hadoop fs -cd /home/jaakko
# Drop mongoDB first
mongo localhost/$DB --eval "db.dropDatabase()"
# Create temporary directory for this MR job
hadoop fs -mkdir eurisco_germ_$date
# First stage, get info from GBIF.
echo -e $getGBIFData > getGBIFData_$date.py
chmod +x getGBIFData_$date.py
echo -e $storeInMongo > storeInMongo_$date.py
chmod +x storeInMongo_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=40 -input $input -mapper getGBIFData_$date.py -file getGBIFData_$date.py -reducer storeInMongo_$date.py -file storeInMongo_$date.py -output eurisco_germ_$date/rawdata
if [ $? != 0 ]; then
	echo "Map Reduce failed!"
	rm getGBIFData_$date.py storeInMongo_$date.py
	exit 1
fi
rm getGBIFData_$date.py storeInMongo_$date.py
