CREATE DATABASE DORE_DACS_SIT3;

USE DORE_DACS_SIT3;


CREATE TABLE `products` (
                            `id` int(11) NOT NULL AUTO_INCREMENT,
                            `name` varchar(255) NOT NULL DEFAULT 'flink',
                            `description` varchar(255) DEFAULT NULL,
                            `weight` decimal(20,10) DEFAULT NULL,
                            PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30001