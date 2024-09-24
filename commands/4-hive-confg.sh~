#!/bin/sh

clean=false

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
		hive -e "USE bitcoin_data;drop table bitcoin_price;"
		hive -f ../config/hive.hql;
	else
		hive -f ../config/hive.hql;
	fi
else
	hive -f ../config/hive.hql;
fi 






