DROP TABLE IF EXISTS `transaction`;
CREATE TABLE `transaction`
(
    `block_number`        INT       NOT NULL DEFAULT 0,
    `time_stamp`          INT       NOT NULL DEFAULT 0,
    `hash`                CHAR(255),
    `nonce`               INT       NOT NULL DEFAULT 0,
    `block_hash`          CHAR(255),
    `transaction_index`   INT       NULL     DEFAULT 0,
    `from`                CHAR(255),
    `to`                  CHAR(255),
    `value`               CHAR(255) NULL     DEFAULT '0',
    `gas`                 CHAR(255),
    `gas_price`           CHAR(255),
    `is_error`            INT       NOT NULL DEFAULT 0,
    `txreceipt_status`    INT       NOT NULL DEFAULT 0,
    `input`               TEXT,
    `contract_address`    CHAR(255),
    `cumulative_gas_used` INT       NOT NULL DEFAULT 0,
    `gas_used`            INT       NOT NULL DEFAULT 0,
    `confirmations`       INT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`hash`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
