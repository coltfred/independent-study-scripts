DROP TABLE geotweets.users;
DROP TABLE geotweets.places;
DROP TABLE geotweets.tweets;


CREATE  TABLE IF NOT EXISTS `geotweets`.`places` (
  `id` VARCHAR(45) NOT NULL ,
  `coord_type` VARCHAR(45) NULL ,
  `coord` GEOMETRY NOT NULL ,
  `name` VARCHAR(45) NULL ,
  `full_name` VARCHAR(45) NULL ,
  `type` VARCHAR(45) NULL ,
  PRIMARY KEY (`id`) ,
  SPATIAL INDEX `coord` (`coord` ASC) )
ENGINE = MyISAM;


CREATE  TABLE IF NOT EXISTS `geotweets`.`users` (
  `id` INT NOT NULL ,
  `created` DATETIME NULL ,
  `description` MEDIUMBLOB NULL ,
  `followers_count` INT NULL ,
  `following_count` INT NULL ,
  `name` VARCHAR(45) NULL ,
  `screen_name` VARCHAR(45) NULL ,
  `statuses_count` INT NULL ,
  `utc_offset` INT NULL ,
  `verified` BOOLEAN NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE  TABLE IF NOT EXISTS `geotweets`.`tweets` (
  `id` VARCHAR(45) NOT NULL ,
  `created` DATETIME NULL ,
  `reply_to_screen_name` VARCHAR(45) NULL ,
  `reply_to_id` VARCHAR(45) NULL ,
  `retweet_count` INT NULL ,
  `text` VARCHAR(200) NULL ,
  `truncated` BOOLEAN NULL ,
  `geo` GEOMETRY NOT NULL ,
  `geo_type` VARCHAR(45) NULL ,
  `reply_to_user_id` INT NULL ,
  `user_id` INT NULL ,
  `place_id` VARCHAR(45) NULL ,
  INDEX `user_id` (`user_id` ASC) ,
  PRIMARY KEY (`id`) ,
  INDEX `place_id` (`place_id` ASC) ,
  SPATIAL INDEX `geo` (`geo` ASC) ,
  CONSTRAINT `user_id`
    FOREIGN KEY (`user_id` )
    REFERENCES `geotweets`.`users` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `place_id`
    FOREIGN KEY (`place_id` )
    REFERENCES `geotweets`.`places` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;