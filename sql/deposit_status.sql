DROP TABLE IF EXISTS `deposit_status`;
CREATE TABLE `deposit_status`
(
    `id`                   INT AUTO_INCREMENT,
    `count`                INT    NOT NULL DEFAULT 0,
    `total_value`          DOUBLE NOT NULL DEFAULT 0,
    `validator_count`      INT    NOT NULL DEFAULT 0,
    `percent_of_count`     DOUBLE NOT NULL DEFAULT 0.0,
    `percent_of_value`     DOUBLE NOT NULL DEFAULT 0.0,
    `percent_of_validator` DOUBLE NOT NULL DEFAULT 0.0,
    `entity`               INT    NOT NULL,
    `timestamp`            INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
