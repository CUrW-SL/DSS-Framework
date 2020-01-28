-- MySQL dump 10.13  Distrib 5.7.28, for Linux (x86_64)
--
-- Host: 35.227.163.211    Database: dss
-- ------------------------------------------------------
-- Server version	5.7.28-0ubuntu0.18.04.4

/*!40101 SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS = @@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION = @@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE = @@TIME_ZONE */;
/*!40103 SET TIME_ZONE = '+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS = @@UNIQUE_CHECKS, UNIQUE_CHECKS = 0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS = 0 */;
/*!40101 SET @OLD_SQL_MODE = @@SQL_MODE, SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES = @@SQL_NOTES, SQL_NOTES = 0 */;

--
-- Table structure for table `accuracy_rules`
--

DROP TABLE IF EXISTS `accuracy_rules`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `accuracy_rules`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `model_type`        varchar(45)  DEFAULT NULL,
    `model`             varchar(45)  DEFAULT NULL,
    `observed_stations` varchar(250) DEFAULT NULL,
    `allowed_error`     float        DEFAULT '0',
    `rule_accuracy`     float        DEFAULT '0',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 3
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `accuracy_rules`
--

LOCK TABLES `accuracy_rules` WRITE;
/*!40000 ALTER TABLE `accuracy_rules`
    DISABLE KEYS */;
INSERT INTO `accuracy_rules`
VALUES (1, 'WRF', 'A', 'IBATTARA2-60, Kottawa North Dharmapala School-55', 60, 100),
       (2, 'FLO2D', '250', 'Janakala Kendraya-65,Diyasaru Uyana-60,Kaduwela Bridge-65,Ingurukade-55', 55, 75);
/*!40000 ALTER TABLE `accuracy_rules`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dynamic_dags`
--

DROP TABLE IF EXISTS `dynamic_dags`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dynamic_dags`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `dag_name`          varchar(45)  DEFAULT NULL,
    `schedule`          varchar(45)  DEFAULT NULL,
    `timeout`           varchar(100) DEFAULT NULL,
    `description`       varchar(100) DEFAULT NULL,
    `status`            int(11)      DEFAULT NULL,
    `last_trigger_date` datetime     DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dynamic_dags`
--

LOCK TABLES `dynamic_dags` WRITE;
/*!40000 ALTER TABLE `dynamic_dags`
    DISABLE KEYS */;
INSERT INTO `dynamic_dags`
VALUES (1, 'dynamic_dag1', '*/10 * * * *', '{\"hours\":0,\"minutes\":2,\"seconds\":30}', 'dynamically created dag', 3,
        '2020-01-28 03:20:36');
/*!40000 ALTER TABLE `dynamic_dags`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dynamic_tasks`
--

DROP TABLE IF EXISTS `dynamic_tasks`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dynamic_tasks`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `task_name`    varchar(45)  NOT NULL,
    `bash_script`  varchar(100) NOT NULL,
    `input_params` varchar(250) DEFAULT NULL,
    `timeout`      varchar(100) NOT NULL,
    `task_order`   int(11)      DEFAULT NULL,
    `active`       tinyint(2)   DEFAULT NULL,
    `dag_id`       int(11)      NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 4
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dynamic_tasks`
--

LOCK TABLES `dynamic_tasks` WRITE;
/*!40000 ALTER TABLE `dynamic_tasks`
    DISABLE KEYS */;
INSERT INTO `dynamic_tasks`
VALUES (1, 'task1', '/home/uwcc-admin/test_bash/task1.sh', '{\"a\":12,\"b\":\"daily\",\"c\":\"2020-01-20 10:30:00\"}',
        '{\"hours\":0,\"minutes\":1,\"seconds\":30}', 1, 1, 1),
       (2, 'task2', '/home/uwcc-admin/test_bash/task2.sh', '{\"d\":\"2020-01-20 10:30:00\"}',
        '{\"hours\":0,\"minutes\":1,\"seconds\":0}', 3, 1, 1),
       (3, 'task3', '/home/uwcc-admin/test_bash/task3.sh', NULL, '{\"hours\":0,\"minutes\":0,\"seconds\":30}', 2, 1, 1);
/*!40000 ALTER TABLE `dynamic_tasks`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flo2d_rules`
--

DROP TABLE IF EXISTS `flo2d_rules`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `flo2d_rules`
(
    `id`                   int(11) NOT NULL AUTO_INCREMENT,
    `name`                 varchar(45)  DEFAULT NULL,
    `status`               int(11)      DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`         varchar(45)  DEFAULT NULL,
    `forecast_days`        int(11)      DEFAULT NULL,
    `observed_days`        int(11)      DEFAULT NULL,
    `no_forecast_continue` tinyint(1)   DEFAULT NULL,
    `no_observed_continue` tinyint(1)   DEFAULT NULL,
    `raincell_data_from`   int(11)      DEFAULT NULL,
    `inflow_data_from`     int(11)      DEFAULT NULL,
    `outflow_data_from`    int(11)      DEFAULT NULL,
    `ignore_previous_run`  tinyint(1)   DEFAULT NULL,
    `accuracy_rule`        int(11)      DEFAULT '0',
    `current_accuracy`     float        DEFAULT '0',
    `rule_details`         varchar(500) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 5
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flo2d_rules`
--

LOCK TABLES `flo2d_rules` WRITE;
/*!40000 ALTER TABLE `flo2d_rules`
    DISABLE KEYS */;
INSERT INTO `flo2d_rules`
VALUES (1, 'rule1', 2, '250m', 2, 3, 1, 0, 1, 0, 0, 1, 2, 0,
        '{\"run_node\":\"10.138.0.4\",\"run_port\":\"8088\", \"outflow_gen_method\":\"code\"}'),
       (2, 'rule2', 3, '150m', 3, 5, 1, 0, 1, 0, 0, 1, 0, 0,
        '{\"run_node\":\"10.138.0.7\",\"run_port\":\"8089\", \"outflow_gen_method\":\"code\"}'),
       (3, 'rule3', 1, '250m', 2, 3, 0, 0, 1, 0, 0, 1, 0, 0, NULL),
       (4, 'rule4', 3, '250m', 2, 3, 1, 0, 3, 0, 0, 1, 0, 0, NULL);
/*!40000 ALTER TABLE `flo2d_rules`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hechms_rules`
--

DROP TABLE IF EXISTS `hechms_rules`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hechms_rules`
(
    `id`                   int(11) NOT NULL AUTO_INCREMENT,
    `name`                 varchar(45)  DEFAULT NULL,
    `status`               int(11)      DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`         varchar(45)  DEFAULT NULL,
    `forecast_days`        int(11)      DEFAULT NULL,
    `observed_days`        int(11)      DEFAULT NULL,
    `init_run`             tinyint(1)   DEFAULT NULL,
    `no_forecast_continue` tinyint(1)   DEFAULT NULL,
    `no_observed_continue` tinyint(1)   DEFAULT NULL,
    `rainfall_data_from`   int(11)      DEFAULT NULL,
    `ignore_previous_run`  tinyint(1)   DEFAULT NULL,
    `accuracy_rule`        int(11)      DEFAULT '0',
    `current_accuracy`     float        DEFAULT '0',
    `rule_details`         varchar(500) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 3
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hechms_rules`
--

LOCK TABLES `hechms_rules` WRITE;
/*!40000 ALTER TABLE `hechms_rules`
    DISABLE KEYS */;
INSERT INTO `hechms_rules`
VALUES (1, 'rule1', 3, 'distributed', 2, 3, 0, 1, 0, 2, 1, 0, 0, '{\"run_node\":\"10.138.0.3\",\"run_port\":\"5000\"}'),
       (2, 'rule2', 3, 'distributed', 2, 3, 0, 0, 0, 1, 1, 0, 0, NULL);
/*!40000 ALTER TABLE `hechms_rules`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `namelist_input_config`
--

DROP TABLE IF EXISTS `namelist_input_config`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `namelist_input_config`
(
    `id`                      int(11) NOT NULL AUTO_INCREMENT,
    `run_days`                int(11)     DEFAULT NULL,
    `run_hours`               int(11)     DEFAULT NULL,
    `run_minutes`             int(11)     DEFAULT NULL,
    `run_seconds`             int(11)     DEFAULT NULL,
    `interval_seconds`        int(11)     DEFAULT NULL,
    `input_from_file`         varchar(45) DEFAULT NULL,
    `history_interval`        varchar(45) DEFAULT NULL,
    `frames_per_outfile`      varchar(45) DEFAULT NULL,
    `restart`                 varchar(45) DEFAULT NULL,
    `restart_interval`        int(11)     DEFAULT NULL,
    `io_form_history`         int(11)     DEFAULT NULL,
    `io_form_restart`         int(11)     DEFAULT NULL,
    `io_form_input`           int(11)     DEFAULT NULL,
    `io_form_boundary`        int(11)     DEFAULT NULL,
    `debug_level`             int(11)     DEFAULT NULL,
    `time_step`               int(11)     DEFAULT NULL,
    `time_step_fract_num`     int(11)     DEFAULT NULL,
    `time_step_fract_den`     int(11)     DEFAULT NULL,
    `max_dom`                 int(11)     DEFAULT NULL,
    `e_we`                    varchar(45) DEFAULT NULL,
    `e_sn`                    varchar(45) DEFAULT NULL,
    `e_vert`                  varchar(45) DEFAULT NULL,
    `p_top_requested`         int(11)     DEFAULT NULL,
    `num_metgrid_levels`      int(11)     DEFAULT NULL,
    `num_metgrid_soil_levels` int(11)     DEFAULT NULL,
    `dx`                      varchar(45) DEFAULT NULL,
    `dy`                      varchar(45) DEFAULT NULL,
    `grid_id`                 varchar(45) DEFAULT NULL,
    `parent_id`               varchar(45) DEFAULT NULL,
    `i_parent_start`          varchar(45) DEFAULT NULL,
    `j_parent_start`          varchar(45) DEFAULT NULL,
    `parent_grid_ratio`       varchar(45) DEFAULT NULL,
    `parent_time_step_ratio`  varchar(45) DEFAULT NULL,
    `feedback`                int(11)     DEFAULT NULL,
    `smooth_option`           int(11)     DEFAULT NULL,
    `mp_physics`              varchar(45) DEFAULT NULL,
    `ra_lw_physics`           varchar(45) DEFAULT NULL,
    `ra_sw_physics`           varchar(45) DEFAULT NULL,
    `radt`                    varchar(45) DEFAULT NULL,
    `sf_sfclay_physics`       varchar(45) DEFAULT NULL,
    `sf_surface_physics`      varchar(45) DEFAULT NULL,
    `bl_pbl_physics`          varchar(45) DEFAULT NULL,
    `bldt`                    varchar(45) DEFAULT NULL,
    `cu_physics`              varchar(45) DEFAULT NULL,
    `cudt`                    varchar(45) DEFAULT NULL,
    `isfflx`                  int(11)     DEFAULT NULL,
    `ifsnow`                  int(11)     DEFAULT NULL,
    `icloud`                  int(11)     DEFAULT NULL,
    `surface_input_source`    int(11)     DEFAULT NULL,
    `num_soil_layers`         int(11)     DEFAULT NULL,
    `sf_urban_physics`        varchar(45) DEFAULT NULL,
    `w_damping`               int(11)     DEFAULT NULL,
    `diff_opt`                int(11)     DEFAULT NULL,
    `km_opt`                  int(11)     DEFAULT NULL,
    `diff_6th_opt`            varchar(45) DEFAULT NULL,
    `diff_6th_factor`         varchar(45) DEFAULT NULL,
    `base_temp`               int(11)     DEFAULT NULL,
    `damp_opt`                int(11)     DEFAULT NULL,
    `epssm`                   float       DEFAULT NULL,
    `zdamp`                   varchar(45) DEFAULT NULL,
    `dampcoef`                varchar(45) DEFAULT NULL,
    `khdif`                   varchar(45) DEFAULT NULL,
    `kvdif`                   varchar(45) DEFAULT NULL,
    `non_hydrostatic`         varchar(45) DEFAULT NULL,
    `moist_adv_opt`           varchar(45) DEFAULT NULL,
    `scalar_adv_opt`          varchar(45) DEFAULT NULL,
    `spec_bdy_width`          int(11)     DEFAULT NULL,
    `spec_zone`               int(11)     DEFAULT NULL,
    `relax_zone`              int(11)     DEFAULT NULL,
    `specified`               varchar(45) DEFAULT NULL,
    `nested`                  varchar(45) DEFAULT NULL,
    `nio_tasks_per_group`     int(11)     DEFAULT NULL,
    `nio_groups`              int(11)     DEFAULT NULL,
    `last_access_date`        datetime    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `namelist_input_config`
--

LOCK TABLES `namelist_input_config` WRITE;
/*!40000 ALTER TABLE `namelist_input_config`
    DISABLE KEYS */;
INSERT INTO `namelist_input_config`
VALUES (1, 3, 0, 0, 0, 10800, '.true.,.true.,.true.', '15,  15,   15', '1000, 1000, 1000', '.false.', 5000, 2, 2, 2, 2,
        0, 180, 0, 1, 3, '80,    103,   100', '90,    121,    163', '35,    35,    35', 5000, 34, 4,
        '27000, 9000,  3000', '27000, 9000,  3000', '1,     2,     3', '1,     1,     2', '1,     24,    35',
        '1,     26,    35', '1,     3,     3', '1,     3,     3', 1, 0, '3,     3,     3', '1,     1,     1',
        '1,     1,     1', '30,    10,    10', '2,     2,     2', '2,     2,     2', '2,     2,     2',
        '0,     0,     0', '2,     2,     0', '5,     5,     5', 1, 0, 1, 1, 4, '0,     0,     0', 0, 1, 4,
        '0,      0,      0', '0.12,   0.12,   0.12', 290, 0, 1, '5000.,  5000.,  5000.', '0.2,    0.2,    0.2',
        '0,      0,      0', '0,      0,      0', '.true., .true., .true.', '1,      1,      1', '1,      1,      1', 5,
        1, 4, '.true., .false.,.false.', '.false., .true., .true.', 0, 1, NULL);
/*!40000 ALTER TABLE `namelist_input_config`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `namelist_wps_config`
--

DROP TABLE IF EXISTS `namelist_wps_config`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `namelist_wps_config`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `wrf_core`          varchar(45) DEFAULT NULL,
    `max_dom`           int(11)     DEFAULT NULL,
    `interval_seconds`  int(11)     DEFAULT NULL,
    `io_form_geogrid`   int(11)     DEFAULT NULL,
    `parent_id`         varchar(45) DEFAULT NULL,
    `parent_grid_ratio` varchar(45) DEFAULT NULL,
    `i_parent_start`    varchar(45) DEFAULT NULL,
    `j_parent_start`    varchar(45) DEFAULT NULL,
    `e_we`              varchar(45) DEFAULT NULL,
    `e_sn`              varchar(45) DEFAULT NULL,
    `geog_data_res`     varchar(45) DEFAULT NULL,
    `dx`                varchar(45) DEFAULT NULL,
    `dy`                varchar(45) DEFAULT NULL,
    `map_proj`          varchar(45) DEFAULT NULL,
    `ref_lat`           float       DEFAULT NULL,
    `ref_lon`           float       DEFAULT NULL,
    `truelat1`          float       DEFAULT NULL,
    `truelat2`          float       DEFAULT NULL,
    `stand_lon`         float       DEFAULT NULL,
    `geog_data_path`    varchar(45) DEFAULT NULL,
    `out_format`        varchar(45) DEFAULT NULL,
    `prefix`            varchar(45) DEFAULT NULL,
    `fg_name`           varchar(45) DEFAULT NULL,
    `io_form_metgrid`   int(11)     DEFAULT NULL,
    `last_access_date`  datetime    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `namelist_wps_config`
--

LOCK TABLES `namelist_wps_config` WRITE;
/*!40000 ALTER TABLE `namelist_wps_config`
    DISABLE KEYS */;
INSERT INTO `namelist_wps_config`
VALUES (1, 'ARW', 3, 10800, 2, '1,   1, 2', '1,   3, 3', '1,  24, 35', '1,  26, 35', '80, 103, 100', '90, 121, 163',
        '\'10m\',\'5m\',\'2m\'', '27000', '27000', 'mercator', 7.697, 80.774, 7.697, 0, 80.774, 'GEOG', 'WPS', 'FILE',
        'FILE', 2, NULL);
/*!40000 ALTER TABLE `namelist_wps_config`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pump_rules`
--

DROP TABLE IF EXISTS `pump_rules`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pump_rules`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `rule_name`         varchar(45)   DEFAULT NULL,
    `rule_logic`        varchar(5000) DEFAULT NULL,
    `dag_name`          varchar(100)  DEFAULT NULL,
    `status`            int(11)       DEFAULT NULL,
    `schedule`          varchar(45)   DEFAULT NULL,
    `last_trigger_time` datetime      DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 2
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pump_rules`
--

LOCK TABLES `pump_rules` WRITE;
/*!40000 ALTER TABLE `pump_rules`
    DISABLE KEYS */;
INSERT INTO `pump_rules`
VALUES (1, 'rule1',
        '((location_name=\'Yakbedda\') and (variable_type=\'WaterLevel\') and ((current_water_level>=alert_water_level) or (current_water_level>=warning_water_level))) or ((location_name=\'Kohuwala\') and (variable_type=\'Precipitation\') and ((rainfall_intensity>=65.4) or ((last_1_day_rainfall>=150) and (last_3_day_rainfall>=420))))',
        'flo2d_150_pump1_dag', 1, '*/5 * * * *', NULL);
/*!40000 ALTER TABLE `pump_rules`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `rule_variables`
--

DROP TABLE IF EXISTS `rule_variables`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rule_variables`
(
    `id`                      int(11)     NOT NULL AUTO_INCREMENT,
    `location_name`           varchar(45) NOT NULL,
    `variable_type`           varchar(45) NOT NULL,
    `current_rainfall`        float DEFAULT NULL,
    `current_water_level`     float DEFAULT NULL,
    `alert_water_level`       float DEFAULT NULL,
    `warning_water_level`     float DEFAULT NULL,
    `water_level_rising_rate` float DEFAULT NULL,
    `rainfall_intensity`      float DEFAULT NULL,
    `last_1_day_rainfall`     float DEFAULT NULL,
    `last_2_day_rainfall`     float DEFAULT NULL,
    `last_3_day_rainfall`     float DEFAULT NULL,
    PRIMARY KEY (`id`, `location_name`, `variable_type`),
    UNIQUE KEY `station_UNIQUE` (`location_name`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 22
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rule_variables`
--

LOCK TABLES `rule_variables` WRITE;
/*!40000 ALTER TABLE `rule_variables`
    DISABLE KEYS */;
INSERT INTO `rule_variables`
VALUES (1, 'Ingurukade', 'WaterLevel', NULL, 0.586, 1, 1.2, NULL, NULL, NULL, NULL, NULL),
       (2, 'Yakbedda', 'WaterLevel', NULL, 0.636, 1.2, 1.7, NULL, NULL, NULL, NULL, NULL),
       (3, 'Janakala Kendraya', 'WaterLevel', NULL, 0.404, 1.7, 2, NULL, NULL, NULL, NULL, NULL),
       (4, 'Wellampitiya', 'WaterLevel', NULL, 0.524, 1, 1.2, NULL, NULL, NULL, NULL, NULL),
       (5, 'Diyasaru Uyana', 'WaterLevel', NULL, 0.997, 1.7, 2, NULL, NULL, NULL, NULL, NULL),
       (6, 'Wellawatta', 'WaterLevel', NULL, 0.253, 1.7, 2, NULL, NULL, NULL, NULL, NULL),
       (7, 'Ambewela Farm', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (8, 'Kottawa North Dharmapala School', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (9, 'Kotikawatta', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (10, 'Dickoya', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (11, 'Kohuwala', 'Precipitation', 0, NULL, NULL, NULL, NULL, -9999, NULL, NULL, NULL),
       (12, 'Hingurana Study', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (13, 'Jaffna Study', 'Precipitation', 0, NULL, NULL, NULL, NULL, -9999, NULL, NULL, NULL),
       (14, 'Uduwawala', 'Precipitation', -9999, NULL, NULL, NULL, NULL, -9999, NULL, NULL, NULL),
       (15, 'Orugodawatta', 'Precipitation', -9999, NULL, NULL, NULL, NULL, -9999, NULL, NULL, NULL),
       (16, 'IBATTARA2', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (17, 'Aruwakkalu', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (18, 'Makumbura', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (19, 'Gonawala', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (20, 'Arangala', 'Precipitation', 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
       (21, 'Mutwal Study', 'Precipitation', -9999, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
/*!40000 ALTER TABLE `rule_variables`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `variable_routines`
--

DROP TABLE IF EXISTS `variable_routines`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `variable_routines`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `variable_name`     varchar(45) DEFAULT NULL,
    `variable_type`     varchar(45) DEFAULT NULL,
    `dag_name`          varchar(45) DEFAULT NULL,
    `status`            int(11)     DEFAULT NULL,
    `schedule`          varchar(45) DEFAULT NULL,
    `last_trigger_date` datetime    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 8
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `variable_routines`
--

LOCK TABLES `variable_routines` WRITE;
/*!40000 ALTER TABLE `variable_routines`
    DISABLE KEYS */;
INSERT INTO `variable_routines`
VALUES (1, 'current_rainfall', 'Precipitation', 'current_rainfall_dag', 3, '*/10 * * * *', '2020-01-28 03:21:01'),
       (2, 'current_water_level', 'WaterLevel', 'current_water_level_dag', 3, '*/10 * * * *', '2020-01-28 03:21:01'),
       (3, 'water_level_rising_rate', 'WaterLevel', 'water_level_rising_rate_dag', 3, '*/20 * * * *',
        '2020-01-28 03:21:01'),
       (4, 'last_1_day_rainfall', 'Precipitation', 'last_1_day_rain_dag', 3, '0 * * * *', '2020-01-28 03:00:52'),
       (5, 'last_2_day_rainfall', 'Precipitation', 'last_2_day_rain_dag', 3, '0 */6 * * *', '2020-01-28 00:00:36'),
       (6, 'last_3_day_rainfall', 'Precipitation', 'last_3_day_rain_dag', 3, '0 */6 * * *', '2020-01-28 00:00:36'),
       (7, 'rainfall_intensity', 'Precipitation', 'rainfall_intensity_dag', 3, '*/10 * * * *', '2020-01-28 03:21:01');
/*!40000 ALTER TABLE `variable_routines`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `workflow_routines`
--

DROP TABLE IF EXISTS `workflow_routines`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `workflow_routines`
(
    `id`                int(11) NOT NULL AUTO_INCREMENT,
    `dss1`              int(11)     DEFAULT NULL COMMENT 'rule id related to wrf',
    `dss2`              int(11)     DEFAULT NULL COMMENT 'rule id related to hechms',
    `dss3`              int(11)     DEFAULT NULL COMMENT 'rule id related to flo2d',
    `status`            int(11)     DEFAULT NULL COMMENT '0-not triggered, 1- triggered, 2- running, 3- completed, 4- error',
    `cascade_on`        tinyint(1)  DEFAULT NULL,
    `schedule`          varchar(45) DEFAULT NULL,
    `last_trigger_date` datetime    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 34
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `workflow_routines`
--

LOCK TABLES `workflow_routines` WRITE;
/*!40000 ALTER TABLE `workflow_routines`
    DISABLE KEYS */;
INSERT INTO `workflow_routines`
VALUES (1, 1, 0, 0, 0, 0, '32 00 * * *', '2019-12-27 00:35:37'),
       (2, 2, 0, 0, 0, 0, '30 11 * * *', '2019-12-26 11:30:26'),
       (3, 3, 0, 0, 0, 0, '30 17 * * *', '2019-12-26 17:30:38'),
       (4, 4, 0, 0, 0, 0, '30 23 * * *', '2019-12-26 23:30:37'),
       (5, 1, 0, 0, 0, 0, '00 04 * * *', '2019-12-27 02:20:22'),
       (6, 2, 0, 0, 0, 0, '00 04 * * *', '2019-12-27 02:20:22'),
       (7, 3, 0, 0, 0, 0, '00 04 * * *', '2019-12-27 02:20:22'),
       (8, 4, 0, 0, 0, 0, '00 04 * * *', '2019-12-27 02:20:22'),
       (9, 1, 0, 0, 0, 0, '*/5 * * * *', '2019-12-26 09:15:35'),
       (10, 0, 0, 1, 0, 0, '*/30 * * * *', '2019-12-31 05:30:23'),
       (11, 13, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (12, 14, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (13, 15, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (14, 16, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (15, 1, 0, 0, 3, 0, '30 07 * * *', '2020-01-27 07:30:43'),
       (16, 2, 0, 0, 3, 0, '30 07 * * *', '2020-01-27 07:30:43'),
       (17, 3, 0, 0, 3, 0, '30 07 * * *', '2020-01-27 07:30:43'),
       (18, 4, 0, 0, 3, 0, '30 07 * * *', '2020-01-27 07:30:43'),
       (19, 13, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (20, 14, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (21, 15, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (22, 16, 0, 0, 3, 0, '00 16 * * *', '2020-01-27 16:00:29'),
       (23, 0, 0, 1, 2, 0, '20 * * * *', '2020-01-28 03:20:55'),
       (24, 0, 1, 0, 3, 0, '15 * * * *', '2020-01-28 03:15:33'),
       (25, 0, 0, 2, 3, 0, '0 */6 * * *', '2020-01-28 00:00:32'),
       (26, 9, 0, 0, 2, 0, '45 23 * * *', '2020-01-27 23:45:47'),
       (27, 10, 0, 0, 2, 0, '45 23 * * *', '2020-01-27 23:45:47'),
       (28, 11, 0, 0, 2, 0, '45 23 * * *', '2020-01-27 23:45:47'),
       (29, 12, 0, 0, 2, 0, '45 23 * * *', '2020-01-27 23:45:47'),
       (30, 5, 0, 0, 3, 0, '30 10 * * *', '2020-01-27 10:30:42'),
       (31, 6, 0, 0, 3, 0, '30 10 * * *', '2020-01-27 10:30:42'),
       (32, 7, 0, 0, 3, 0, '30 10 * * *', '2020-01-27 10:30:42'),
       (33, 8, 0, 0, 3, 0, '30 10 * * *', '2020-01-27 10:30:42');
/*!40000 ALTER TABLE `workflow_routines`
    ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `wrf_rules`
--

DROP TABLE IF EXISTS `wrf_rules`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wrf_rules`
(
    `id`                          int(11) NOT NULL AUTO_INCREMENT,
    `name`                        varchar(45)  DEFAULT NULL,
    `status`                      int(11)      DEFAULT NULL COMMENT '0 - disable, 1 - enable, 2 - running, 3 - completed',
    `target_model`                varchar(45)  DEFAULT NULL,
    `version`                     varchar(45)  DEFAULT NULL,
    `run`                         varchar(45)  DEFAULT NULL,
    `hour`                        varchar(45)  DEFAULT NULL,
    `ignore_previous_run`         tinyint(1)   DEFAULT NULL,
    `check_gfs_data_availability` tinyint(1)   DEFAULT NULL,
    `accuracy_rule`               int(11)      DEFAULT '0',
    `current_accuracy`            float        DEFAULT '0',
    `rule_details`                varchar(500) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 17
  DEFAULT CHARSET = latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `wrf_rules`
--

LOCK TABLES `wrf_rules` WRITE;
/*!40000 ALTER TABLE `wrf_rules`
    DISABLE KEYS */;
INSERT INTO `wrf_rules`
VALUES (1, 'rule1', 3, 'A', '4.1.2', '1', '00', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (2, 'rule2', 3, 'C', '4.1.2', '1', '00', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (3, 'rule3', 3, 'E', '4.1.2', '1', '00', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (4, 'rule4', 3, 'SE', '4.1.2', '1', '00', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (5, 'rule5', 3, 'A', '4.1.2', '1', '06', 1, 1, 0, 0,
        '{\"run_node\":\"10.128.0.5\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (6, 'rule6', 3, 'C', '4.1.2', '1', '06', 1, 1, 0, 0,
        '{\"run_node\":\"10.128.0.5\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (7, 'rule7', 3, 'E', '4.1.2', '1', '06', 1, 1, 0, 0,
        '{\"run_node\":\"10.128.0.5\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (8, 'rule8', 3, 'SE', '4.1.2', '1', '06', 1, 1, 0, 0,
        '{\"run_node\":\"10.128.0.5\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (9, 'rule9', 2, 'A', '4.1.2', '1', '18', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (10, 'rule10', 2, 'C', '4.1.2', '1', '18', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (11, 'rule11', 2, 'E', '4.1.2', '1', '18', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (12, 'rule12', 2, 'SE', '4.1.2', '1', '18', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (13, 'rule13', 3, 'A', '4.1.2', '1', '12', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (14, 'rule14', 3, 'C', '4.1.2', '1', '12', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (15, 'rule15', 3, 'E', '4.1.2', '1', '12', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}'),
       (16, 'rule16', 3, 'SE', '4.1.2', '1', '12', 1, 1, 0, 0,
        '{\"run_node\":\"10.138.0.10\",\"run_script\":\"/home/uwcc-admin/wrf_docker/runner.sh\",\"push_node\":\"10.138.0.13\",\"push_script\":\"/home/uwcc-admin/curw_wrf_data_pusher/wrf_data_pusher_seq.sh\",\"push_config\":\"/home/uwcc-admin/curw_wrf_data_pusher/config/dwrf_config.json\",\"wrf_bucket\":\"/mnt/disks/wrf_nfs/wrf\"}');
/*!40000 ALTER TABLE `wrf_rules`
    ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE = @OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE = @OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS = @OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS = @OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT = @OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS = @OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION = @OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES = @OLD_SQL_NOTES */;

-- Dump completed on 2020-01-28  8:57:02
