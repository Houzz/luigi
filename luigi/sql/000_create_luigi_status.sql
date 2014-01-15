CREATE TABLE IF NOT EXISTS `job_status` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` varchar(255),
  `status` tinyint unsigned DEFAULT 0,
  `created` datetime DEFAULT NULL,
  `modified` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`task_id`),
  KEY (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
