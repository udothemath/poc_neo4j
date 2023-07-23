echo "Hi. Welcome aboard!"
FILE_PATH=/Users/pro/neo4j-community-4.4.23
echo "FILE_PATH: $FILE_PATH" && echo "OK" || echo "NOK"
sudo cp neo4j_v4423.conf "$FILE_PATH/conf/neo4j.conf" && echo "OK" || echo "NOK"
( sudo rm -Rf $FILE_PATH/data/databases/* $FILE_PATH/data/transactions/* ) && echo "OK" || echo "NOK"
( sudo neo4j console & ) && echo "OK" || echo "NOK"
( echo "You have reset your neo4j.conf setting" ) && echo "OK" || echo "NOK"


