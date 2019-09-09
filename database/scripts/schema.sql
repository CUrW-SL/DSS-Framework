CREATE TABLE IF NOT EXISTS `wrf_rules` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `priority` int(11) DEFAULT NULL,
  `status` int(11) DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
  `target_model` varchar(45) DEFAULT NULL,
  `version` varchar(45) DEFAULT NULL,
  `run` varchar(45) DEFAULT NULL,
  `data_hour` varchar(45) DEFAULT NULL,
  `ignore_previous_run` TINYINT(1) DEFAULT NULL,
  `check_gfs_data_availability` TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `hechms_rules`
(
    `id`                          int(11) NOT NULL AUTO_INCREMENT,
    `name`                        varchar(45) DEFAULT NULL,
    `priority`                    int(11)     DEFAULT NULL,
    `status`                      int(11)     DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`                varchar(45) DEFAULT NULL,
    `forecast_days`               int(11) DEFAULT NULL,
    `observed_days`               int(11) DEFAULT NULL,
    `init_run`                    TINYINT(1) DEFAULT NULL,
    `no_forecast_continue`        TINYINT(1) DEFAULT NULL,
    `no_observed_continue`        TINYINT(1) DEFAULT NULL,
    `rainfall_data_from`          varchar(100) DEFAULT NULL,
    `ignore_previous_run`         TINYINT(1)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    PRIMARY KEY (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `flo2d_rules`
(
    `id`                          int(11) NOT NULL AUTO_INCREMENT,
    `name`                        varchar(45)  DEFAULT NULL,
    `priority`                    int(11)      DEFAULT NULL,
    `status`                      int(11)      DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`                varchar(45) DEFAULT NULL,
    `forecast_days`               int(11)      DEFAULT NULL,
    `observed_days`               int(11)      DEFAULT NULL,
    `init_run`                    TINYINT(1)   DEFAULT NULL,
    `no_forecast_continue`        TINYINT(1)   DEFAULT NULL,
    `no_observed_continue`        TINYINT(1)   DEFAULT NULL,
    `rain_cell_data_from`          varchar(100) DEFAULT NULL,
    `inflow_data_from`          varchar(100) DEFAULT NULL,
    `outflow_data_from`          varchar(100) DEFAULT NULL,
    `ignore_previous_run`         TINYINT(1)   DEFAULT NULL,
    PRIMARY KEY (`id`),
    PRIMARY KEY (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `accuracy_rules`
(
    `id`                  INT          NOT NULL AUTO_INCREMENT,
    `name`                VARCHAR(45)  NOT NULL,
    `priority`            INT          NULL,
    `status`              INT          NULL COMMENT '0-disable,1-enable,2-running,3-completed',
    `variable`            INT          NULL COMMENT '1-precipitation, 2- discharge, 3- water_level, 4- tide',
    `relevant_station`    VARCHAR(200) NULL,
    `accuracy_level`      INT          NULL,
    `accuracy_percentage` INT          NULL,
    PRIMARY KEY (`id`, `name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `water_level_rules`
(
    `id`            INT         NOT NULL AUTO_INCREMENT,
    `station_name`  VARCHAR(45) NOT NULL,
    `alert_level`   DECIMAL     NULL,
    `warning_level` DECIMAL     NULL,
    PRIMARY KEY (`id`, `station_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;