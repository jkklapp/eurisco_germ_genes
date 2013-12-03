#!/bin/bash
####################################################################
####### Jaakko Lappalainen. jkk.lapp@gmail.com - 2013 #############
####################################################################
if [ $# != 2 ];then
	echo "usage: fillDB.sh <hdfs-input-file> <target-mongo-db>"
	exit 1
fi

####### PARAMETERS
date=`date +%s`
DB=$2
getFullRecFromIds='#!/usr/bin/env python\n\r\n\rimport urllib2\n\rimport xmltodict, json\n\rimport sys\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tsp,id = line.split("|",1)\n\r\trequest = urllib2.Request("http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nucleotide&retmode=xml&id="+id)\n\r\tresponse_body = urllib2.urlopen(request).read()\n\r\to = xmltodict.parse(response_body)\n\r\to = json.dumps(o)\n\r\tprint>>sys.stdout, "%s|%s" % (sp.replace("+","-"),o)\n'
getNucIdsMap="#!/usr/bin/env python\n\rimport urllib2\n\rimport xml.etree.ElementTree as ET\n\rimport sys\n\rimport json\n\r\n\rfor line in sys.stdin:\n\r\ttry:\n\r\t\tdoc=json.loads(line.replace(\"u'\",'\"').replace(\"'\",'\"').replace('XXXX',''))\n\r\t\trequest = urllib2.Request(\"http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nucleotide&term=\"+doc[\"scientificName\"]+\"&RetMax=0\")\n\r\t\tresponse_body = urllib2.urlopen(request).read()\n\r\t\troot = ET.fromstring(response_body)\n\r\t\tcount = root[0].text\n\r\t\trequest = urllib2.Request(\"http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=nucleotide&term=\"+doc[\"scientificName\"]+\"&RetMax=\"+count)\n\r\t\tresponse_body = urllib2.urlopen(request).read()\n\r\t\troot = ET.fromstring(response_body)\n\r\t\tfor id in root.find(\".//IdList\"):\n\r\t\t\tid = id.text\n\r\t\t\tprint>>sys.stdout, \"%s|%s\" % (doc[\"scientificName\"].replace(\" \",\"+\"),id)\n\r\texcept:\n\r\t\tcontinue\n"
getGBIFData='#!/usr/bin/env python\n\rimport urllib2\n\rimport json\n\rimport sys\n\rbase = "http://api.gbif.org/v0.9/occurrence/search?datasetKey=865df020-f762-11e1-a439-00145eb45e9a&limit=20&offset="\n\r\n\rfor d in sys.stdin:\n\r\tresponse = urllib2.urlopen(base+str(int(d)*20))\n\r\tcontent = response.read()\n\r\tdoc = json.loads(content)\n\r\tfor r in doc["results"]:\n\r\t\tprint r\n'
storeInMongo="#!/usr/bin/env python\n\rimport sys,json\n\rfrom pymongo import MongoClient\n\r\n\rclient = MongoClient('localhost')\n\rdb = client."$DB"\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tsp,data = line.split('|',1)\n\r\tcol = db[sp.replace('.','')]\n\r\ttry:\n\r\t\tdoc=json.loads(data.replace(\"u'\",'\"').replace(\"'\",'\"').replace('XXXX',''))\n\r\texcept:\n\r\t\tcontinue\n\r\tcol.insert(doc)\n"
justPrint='#!/usr/bin/env python\n\rfrom operator import itemgetter\n\rimport sys\n\r\n\rcurrent_word = None\n\rword = None\n\r\n\rfor line in sys.stdin:\n\r\tline = line.strip()\n\r\tword = line\n\r\tif current_word is None or current_word != word:\n\r\t\tcurrent_word = word\n\r\t\tprint "%s" % (line)\n'
mrstream="/usr/lib/hadoop-mapreduce/hadoop-streaming-2.0.2-alpha.jar"
input="$1"

# Drop mongoDB first
mongo localhost/$DB --eval "db.dropDatabase()"

hadoop fs -mkdir eurisco_germ_$date
echo -e $getGBIFData > getGBIFData_$date.py
chmod +x getGBIFData_$date.py
echo -e $justPrint > justPrint_$date.py
chmod +x justPrint_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=100 -input $input -mapper getGBIFData_$date.py -file getGBIFData_$date.py -reducer justPrint_$date.py -file justPrint_$date.py -output eurisco_germ_$date/species
rm getGBIFData_$date.py
echo -e $getNucIdsMap > getNucIdsMap_$date.py
sed -i "s/XXXX/\\\'/g" getNucIdsMap_$date.py
chmod +x getNucIdsMap_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=100 -input eurisco_germ_$date/species -mapper getNucIdsMap_$date.py  -file getNucIdsMap_$date.py -reducer justPrint_$date.py -file justPrint_$date.py -output eurisco_germ_$date/ids
rm getNucIdsMap_$date.py justPrint_$date.py
echo -e $storeInMongo > storeInMongo_$date.py
sed -i "s/XXXX/\\\'/g" storeInMongo_$date.py
chmod +x storeInMongo_$date.py
echo -e $getFullRecFromIds > getFullRecFromId_$date.py
chmod +x getFullRecFromId_$date.py
hadoop jar $mrstream -D mapreduce.job.maps=50 -D mapreduce.map.memory.mb="4096" -D mapred.child.java.opts="-Xmx1024M" -input eurisco_germ_$date/ids -mapper getFullRecFromId_$date.py -file getFullRecFromId_$date.py -reducer storeInMongo_$date.py -file storeInMongo_$date.py -output eurisco_germ/final_$date
rm storeInMongo_$date.py getFullRecFromId_$date.py
hadoop fs -rm -r eurisco_germ_$date
