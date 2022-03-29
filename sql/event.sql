DROP TABLE IF EXISTS `event`;
CREATE TABLE `event`
(
    `address`           CHAR(255),
    `topics`            CHAR(255),
    `data`              TEXT,
    `block_number`      INT NOT NULL DEFAULT 0,
    `time_stamp`        INT NOT NULL DEFAULT 0,
    `gas_price`         CHAR(255),
    `gas_used`          CHAR(255),
    `log_index`         INT NOT NULL DEFAULT 0,
    `transaction_hash`  CHAR(255),
    `transaction_index` INT NOT NULL DEFAULT 0,
    PRIMARY KEY (`transaction_hash`,`log_index`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

alter table event add  column public_key char(100) DEFAULT '';