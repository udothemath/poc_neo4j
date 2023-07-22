echo "Hi. Welcome aboard!"
PATH=/Users/pro/neo4j-community-4.4.23
echo "File path: $PATH" && echo "OK" || echo "NOK"
echo "hi" && echo "OK" || echo "NOK"
(sudo cp neo4j_v4423.conf '$PATH/conf/neo4j.conf') && echo "OK" || echo "NOK"

if [ cp "neo4j_v4423.conf" "$PATH/conf/neo4j.conf" ] ; then
    echo "Command succeeded"
else
    echo "Command failed"
fi
# if [ "$foo" = "1" ] ; then
#   foo=18
# fi

# sudo rm -Rf $PATH/data/databases/* $PATH/data/transactions/*
# sudo neo4j console &
# echo "You have reset your neo4j.conf setting"


if [ $? == 0 ] ; then
  echo '<the output message you want to display>'
else 
  echo '<failure message>'
fi