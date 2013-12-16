#!/bin/bash
####################################################################
####### Jaakko Lappalainen. jkk.lapp@gmail.com - 2013 #############
####################################################################
if [ $# != 3 ];then
	echo "usage: gbif_genebank_mongo.sh <hdfs-input-file> <target-mongo-db> <ncbi-db>"
	exit 1
fi

####### PARAMETERS
date=`date +%s`
input="$1"
DB=$2
GBIF_DB=$3
# Mappers and reducers' templates
getFullRecFromIds='#!/usr/bin/env python\n\r\n\rimport urllib2\n\rimport xmltodict, json\n\rimport sys\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tsp,id = line.split("|",1)\n\r\trequest = urllib2.Request("http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db='$GBIF_DB'&retmode=xml&id="+id)\n\r\tresponse_body = urllib2.urlopen(request).read()\n\r\to = xmltodict.parse(response_body)\n\r\to = json.dumps(o)\n\r\tprint>>sys.stdout, "%s|%s" % (sp.replace("+","-"),o)\n'
getNucIdsMap="#!/usr/bin/env python\n\rimport urllib2\n\rimport xml.etree.ElementTree as ET\n\rimport sys\n\rimport json\n\r\n\rfor line in sys.stdin:\n\r\tline=line.replace(\" \",\"+\").replace(\"\\\n\",\"\")\n\r\trequest = urllib2.Request(\"http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db="$GBIF_DB"&term=\"+line+\"&RetMax=0\")\n\r\tresponse_body = urllib2.urlopen(request).read()\n\r\troot = ET.fromstring(response_body)\n\r\tcount = root[0].text\n\r\trequest = urllib2.Request(\"http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db="$GBIF_DB"&term=\"+line+\"&RetMax=\"+count)\n\r\tresponse_body = urllib2.urlopen(request).read()\n\r\troot = ET.fromstring(response_body)\n\r\tfor id in root.find(\".//IdList\"):\n\r\t\tid = id.text\n\r\t\tprint>>sys.stdout, \"%s|%s\" % (line,id)\n"
getGBIFData='#!/usr/bin/env python\n\rimport urllib2\n\rimport json\n\rimport sys\n\rbase = "http://api.gbif.org/v0.9/occurrence/search?datasetKey=865df020-f762-11e1-a439-00145eb45e9a&limit=20&offset="\n\r\n\rfor d in sys.stdin:\n\r\tresponse = urllib2.urlopen(base+str(int(d)*20))\n\r\tcontent = response.read()\n\r\tdoc = json.loads(content)\n\r\tfor r in doc["results"]:\n\r\t\ttry:\n\r\t\t\tprint r["scientificName"]\n\r\t\texcept:\n\r\t\t\tpass\n'
storeInMongo="#!/usr/bin/env python\n\rimport sys,json\n\rfrom pymongo import MongoClient\n\r\n\rclient = MongoClient('localhost')\n\rdb = client."$DB"\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tsp,data = line.split('|',1)\n\r\tcol = db[sp.replace('.','')]\n\r\ttry:\n\r\t\tdoc=json.loads(data.replace(\"u'\",'\"').replace(\"'\",'\"').replace('XXXX',''))\n\r\texcept:\n\r\t\tcontinue\n\r\tcol.insert(doc)\n"
justPrint='#!/usr/bin/env python\n\rfrom operator import itemgetter\n\rimport sys\n\r\n\rcurrent_word = None\n\rword = None\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tword = line\n\r\tif current_word is None or current_word != word:\n\r\t\tcurrent_word = word\n\r\t\tprint "%s" % (line)\n'
mrstream="/usr/lib/hadoop-mapreduce/hadoop-streaming-2.0.2-alpha.jar"
#echo -e $getGBIFData > getGBIFData.py
#echo -e $getFullRecFromIds > getFullRecFromIds.py
#echo -e $getNucIdsMap > getNucIdsMap.py
#echo -e $storeInMongo > storeInMongo.py
#echo -e $justPrint > justPrint.py
#exit


# Drop mongoDB first
mongo localhost/$DB --eval "db.dropDatabase()"
# Create temporary directory for this MR job
hadoop fs -mkdir eurisco_germ_$date
# First stage, get info from GBIF.
echo -e $getGBIFData > getGBIFData_$date.py
chmod +x getGBIFData_$date.py
echo -e $justPrint > justPrint_$date.py
chmod +x justPrint_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=100 -input $input -mapper getGBIFData_$date.py -file getGBIFData_$date.py -reducer justPrint_$date.py -file justPrint_$date.py -output eurisco_germ_$date/species
rm getGBIFData_$date.py
# Second stage: For each species found in GBIF, get available gene data. 
echo -e $getNucIdsMap > getNucIdsMap_$date.py
sed -i "s/XXXX/\\\'/g" getNucIdsMap_$date.py
chmod +x getNucIdsMap_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=100 -input eurisco_germ_$date/species -mapper getNucIdsMap_$date.py  -file getNucIdsMap_$date.py -reducer justPrint_$date.py -file justPrint_$date.py -output eurisco_germ_$date/ids
rm getNucIdsMap_$date.py justPrint_$date.py
# Third stage, store genetic data for each species in a different collection.
echo -e $storeInMongo > storeInMongo_$date.py
sed -i "s/XXXX/\\\'/g" storeInMongo_$date.py
chmod +x storeInMongo_$date.py
echo -e $getFullRecFromIds > getFullRecFromId_$date.py
chmod +x getFullRecFromId_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=25 -D mapreduce.map.memory.mb="8192" -D mapred.child.java.opts="-Xmx512M" -input eurisco_germ_$date/ids -mapper getFullRecFromId_$date.py -file getFullRecFromId_$date.py -reducer storeInMongo_$date.py -file storeInMongo_$date.py -output eurisco_germ/final_$date
rm storeInMongo_$date.py getFullRecFromId_$date.py
# Remove leftovers...
#hadoop fs -rm -r eurisco_germ_$date
