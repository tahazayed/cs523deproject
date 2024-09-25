#!/bin/sh

clean=true

while getopts 'c' opt; do
	case $opt in
		c) clean=true;;
		*) echo "Error unknown arg!" exit 1
	esac
done


echo configuring hive ...
if "$clean";
	then if [[ $(echo "show tables like 'bitcoin_price'" | hive | grep 'bitcoin_price ') ]];
		then echo "deleting bitcoin_price table"
		hive -e "USE bitcoin_data;drop table IF EXISTS bitcoin_price;drop table IF EXISTS bitcoin_price_hbase"
		hive -f ../config/hive.hql;
	else
		hive -f ../config/hive.hql;
	fi
else
	hive -f ../config/hive.hql;
fi 






