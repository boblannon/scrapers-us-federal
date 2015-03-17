#!/bin/bash

the_date=$1
last_date=$(date --date "$2 1 day" +"%Y-%m-%d")

echo $the_date
echo $last_date

until [ "$the_date" == "$last_date" ]
do
    pupa --loglevel ERROR update unitedstates lobbying_registrations start_date=$the_date end_date=$the_date --fastmode;
    the_date=$(date --date "$the_date 1 day" +"%Y-%m-%d");
done
