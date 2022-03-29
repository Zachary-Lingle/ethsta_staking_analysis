DROP TABLE IF EXISTS `internal_transaction`;
CREATE TABLE `internal_transaction`
(
    `block_number`     INT NOT NULL DEFAULT 0,
    `time_stamp`       INT NOT NULL DEFAULT 0,
    `hash`             CHAR(255),
    `from`             CHAR(255),
    `to`               CHAR(255),
    `value`            CHAR(255),
    `contract_address` CHAR(255),
    `input`            CHAR(255),
    `type`             CHAR(255),
    `gas`              CHAR(255),
    `gas_used`         CHAR(255),
    `trace_id`         CHAR(255),
    `is_error`         INT NOT NULL DEFAULT 0,
    `err_code`         CHAR(255),
    PRIMARY KEY (`hash`,`trace_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
