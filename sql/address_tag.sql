DROP TABLE IF EXISTS `address_tag`;
CREATE TABLE `address_tag`
(
    `address`   CHAR(255) NOT NULL DEFAULT '',
    `entity_id` INT       NOT NULL DEFAULT -1,
    `version`   DOUBLE             DEFAULT 1.0,
    PRIMARY KEY (`address`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
