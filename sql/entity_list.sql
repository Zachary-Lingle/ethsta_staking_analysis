DROP TABLE IF EXISTS `entity_list`;
CREATE TABLE `entity_list`
(
    `id`               INT      NOT NULL DEFAULT -1,
    `entity`           CHAR(255),
    `entity_type`      CHAR(50) NOT NULL DEFAULT '',
    `address_count`    INT      NOT NULL DEFAULT 0,
    `public_key_count` INT      NOT NULL DEFAULT 0,
    `tx_type`          CHAR(50) NOT NULL DEFAULT '',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
