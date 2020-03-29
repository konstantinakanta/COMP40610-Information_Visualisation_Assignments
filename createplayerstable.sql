 create table players (surname varchar(60) not null,
 team varchar(60) not null,
 position varchar(60) not null,
 minutes smallint unsigned,
 shots smallint unsigned,
 passes smallint unsigned,
 tackles smallint unsigned,
 saves smallint unsigned,
 FOREIGN KEY (team) REFERENCES teams(team));