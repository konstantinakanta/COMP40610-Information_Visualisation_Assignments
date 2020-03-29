#!/bin/bash
sed 1d Players.csv | while IFS=, read -r surname team position minutes shots passes tackles saves
do
   echo "INSERT INTO players (surname,team,position,minutes,shots,passes,tackles,saves) VALUES ('$surname','$team','$position',$minutes,$shots,$passes,$tackles,$saves);"

done | mysql -u mason --password=um8nv8EB7f -D mason; 