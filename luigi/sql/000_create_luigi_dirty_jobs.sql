CREATE TABLE IF NOT EXISTS `luigi`.`dirty_jobs` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` varchar(255),
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
