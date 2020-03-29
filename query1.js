use mason
db.players.find({minutes:{$lt:200}, team: /ia/ , passes:{$gt:100}}, {surname:1}).pretty()