DROP TABLE IF EXISTS `actor_movies`;
DROP TABLE IF EXISTS `actor_ids`;
DROP TABLE IF EXISTS `genre`;
DROP TABLE IF EXISTS `producer_movies`;
DROP TABLE IF EXISTS `producer_ids`;
DROP TABLE IF EXISTS `movie_info`;


CREATE TABLE  IF NOT EXISTS `movie_info` (
`movie_id` VARCHAR( 100 ) NOT NULL ,
`movie_name` VARCHAR( 100 ) NOT NULL ,
`year` INT ,
`rating` INT,
PRIMARY KEY (  `movie_id` )
) ENGINE = InnoDB;

CREATE TABLE  IF NOT EXISTS `actor_ids` (
`actor_id` VARCHAR( 100 ) NOT NULL ,
`actor_name` VARCHAR( 100 ) NOT NULL ,
`gender` VARCHAR( 100 ) NOT NULL ,
PRIMARY KEY (  `actor_id` )
) ENGINE = InnoDB;


CREATE TABLE  IF NOT EXISTS `actor_movies` (
`actor_id` VARCHAR( 100 ) NOT NULL ,
`movie_id` VARCHAR( 100 ) NOT NULL ,
PRIMARY KEY (  `actor_id`, `movie_id` ),
FOREIGN KEY(`actor_id`) REFERENCES actor_ids(`actor_id`) ON DELETE CASCADE,
FOREIGN KEY(`movie_id`) REFERENCES movie_info(`movie_id`) ON DELETE CASCADE
) ENGINE = InnoDB;


CREATE TABLE  IF NOT EXISTS `producer_ids` (
`producer_id` VARCHAR( 100 ) NOT NULL ,
`producer_name` VARCHAR( 100 ) NOT NULL ,
PRIMARY KEY (  `producer_id` )
) ENGINE = InnoDB;


CREATE TABLE  IF NOT EXISTS `producer_movies` (
`producer_id` VARCHAR( 100 ) NOT NULL ,
`movie_id` VARCHAR( 100 ) NOT NULL ,
PRIMARY KEY (  `producer_id`, `movie_id` ),
FOREIGN KEY(`producer_id`) REFERENCES producer_ids(`producer_id`) ON DELETE CASCADE,
FOREIGN KEY(`movie_id`) REFERENCES movie_info(`movie_id`) ON DELETE CASCADE
) ENGINE = InnoDB;


CREATE TABLE  IF NOT EXISTS `genre` (
`movie_id` VARCHAR( 100 ) NOT NULL ,
`genre` VARCHAR( 100 ) NOT NULL ,
PRIMARY KEY (  `movie_id` ),
FOREIGN KEY(`movie_id`) REFERENCES movie_info(`movie_id`) ON DELETE CASCADE
) ENGINE = InnoDB;



