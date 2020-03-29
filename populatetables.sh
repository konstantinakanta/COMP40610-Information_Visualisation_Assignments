#!/bin/bash
sed 1d Teams.csv | while IFS=, read -r team ranking games wins draws losses goalsFor goalsAgainst yellowCards redCards
do
   echo "INSERT INTO teams (team,ranking,games,wins,draws,losses,goalsFor,goalsAgainst,yellowCards,redCards) VALUES ('$team',$ranking,$games,$wins,$draws,$losses,$goalsFor,$goalsAgainst,$yellowCards,$redCards);"

done | mysql -u mason --password=um8nv8EB7f -D mason;