 create table teams (team varchar(60) not null,
 ranking smallint unsigned,
 games smallint unsigned,
 wins smallint unsigned,
 draws smallint unsigned,
 losses smallint unsigned,
 goalsFor smallint unsigned,
 goalsAgainst smallint unsigned,
 yellowCards smallint unsigned,
 redCards smallint unsigned,
 primary key (team));

 INSERT INTO mason.teams (team,ranking,games,wins,draws,losses,goalsFor,goalsAgainst,yellowCards,redCards) VALUES ('North Korea','105','3','0','0','3','1','12','2','0')