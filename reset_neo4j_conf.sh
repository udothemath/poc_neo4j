echo "Hi. Welcome aboard!"
sudo rm -Rf /var/lib/neo4j/data/databases/* /var/lib/neo4j/data/transactions/*
sudo cp neo4j.conf /etc/neo4j/neo4j.conf
echo "You have reset your neo4j.conf setting"