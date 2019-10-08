CREATE TABLE IF NOT EXISTS `workflow_routines`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `dss1`              int(11)  DEFAULT NULL COMMENT 'rule id related to wrf',
    `dss2`              int(11)  DEFAULT NULL COMMENT 'rule id related to hechms',
    `dss3`              int(11)  DEFAULT NULL COMMENT 'rule id related to flo2d',
    `status`            int(11)  DEFAULT NULL COMMENT '0-not triggered, 1- triggered, 2- running, 3- completed, 4- error',
    `schedule`          varchar(45) DEFAULT NULL,
    `last_trigger_date` datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 3
  DEFAULT CHARSET = latin1;


CREATE TABLE IF NOT EXISTS `wrf_rules`
(
      `id`                          int(11) NOT NULL AUTO_INCREMENT,
      `name`                        varchar(45) DEFAULT NULL,
      `status`                      int(11) DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
      `target_model`                varchar(45) DEFAULT NULL,
      `version`                     varchar(45) DEFAULT NULL,
      `run`                         varchar(45) DEFAULT NULL,
      `hour`                        varchar(45) DEFAULT NULL,
      `ignore_previous_run`         TINYINT(1) DEFAULT NULL,
      `check_gfs_data_availability` TINYINT(1) DEFAULT NULL,
      PRIMARY KEY (`id`),
      PRIMARY KEY (`name`)
) ENGINE=InnoDB
  DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `hechms_rules`
(
    `id`                          int(11) NOT NULL AUTO_INCREMENT,
    `name`                        varchar(45) DEFAULT NULL,
    `status`                      int(11)  DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`                varchar(45) DEFAULT NULL,
    `forecast_days`               int(11) DEFAULT NULL,
    `observed_days`               int(11) DEFAULT NULL,
    `init_run`                    TINYINT(1) DEFAULT NULL,
    `no_forecast_continue`        TINYINT(1) DEFAULT NULL,
    `no_observed_continue`        TINYINT(1) DEFAULT NULL,
    `rainfall_data_from`          int(11) DEFAULT NULL,
    `ignore_previous_run`         TINYINT(1)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    PRIMARY KEY (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `flo2d_rules`
(
    `id`                          int(11) NOT NULL AUTO_INCREMENT,
    `name`                        varchar(45)  DEFAULT NULL,
    `status`                      int(11) DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`                varchar(45) DEFAULT NULL,
    `forecast_days`               int(11) DEFAULT NULL,
    `observed_days`               int(11) DEFAULT NULL,
    `no_forecast_continue`        TINYINT(1) DEFAULT NULL,
    `no_observed_continue`        TINYINT(1) DEFAULT NULL,
    `rain_cell_data_from`         int(11) DEFAULT NULL,
    `inflow_data_from`            int(11) DEFAULT NULL,
    `outflow_data_from`           int(11) DEFAULT NULL,
    `ignore_previous_run`         TINYINT(1) DEFAULT NULL,
    PRIMARY KEY (`id`),
    PRIMARY KEY (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `accuracy_rules`
(
    `id`                  INT NOT NULL AUTO_INCREMENT,
    `name`                VARCHAR(45) NOT NULL,
    `priority`            INT NULL,
    `status`              INT NULL COMMENT '0-disable,1-enable,2-running,3-completed',
    `variable`            INT NULL COMMENT '1-precipitation, 2- discharge, 3- water_level, 4- tide',
    `relevant_station`    VARCHAR(200) NULL,
    `accuracy_level`      INT NULL,
    `accuracy_percentage` INT NULL,
    PRIMARY KEY (`id`, `name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `variable_limits`
(
    `id`                INT NOT NULL AUTO_INCREMENT,
    `station_name`      VARCHAR(45) NOT NULL,
    `variable_type`     DECIMAL NULL COMMENT '1-precipitation, 2- discharge, 3- water_level',
    `alert_level`       DECIMAL NULL,
    `warning_level`     DECIMAL NULL,
    PRIMARY KEY (`id`, `station_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `variable_rules`
(
    `id`                   INT NOT NULL,
    `variable_name`        VARCHAR(45) NOT NULL,
    `check_alert_level`    TINYINT(1) NULL,
    `check_warnning_level` TINYINT(1) NULL,
    PRIMARY KEY (`id`, `variable_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;