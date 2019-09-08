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
  PRIMARY KEY (`id`)
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
    `check_gfs_data_availability` TINYINT(1)  DEFAULT NULL,
    PRIMARY KEY (`id`)
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
    `rainfall_data_from`          varchar(100) DEFAULT NULL,
    `ignore_previous_run`         TINYINT(1)   DEFAULT NULL,
    `check_gfs_data_availability` TINYINT(1)   DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = latin1;