CREATE TABLE `stocks` (
  `id` int NOT NULL,
  `symbol` varchar(10) DEFAULT NULL,
  `short_name` varchar(255) DEFAULT NULL,
  `website` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `symbol` (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;