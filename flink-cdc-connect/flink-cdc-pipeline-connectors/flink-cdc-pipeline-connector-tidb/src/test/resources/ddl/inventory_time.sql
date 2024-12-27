-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE inventory;

USE inventory;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products
(
    id          INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL DEFAULT 'flink',
    description VARCHAR(512),
    weight      DECIMAL(20, 10),
    pt_timestamp datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '时间戳'

);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products(name,description,weight)
VALUES ("scooter", "Small 2-wheel scooter", 3.14),
       ("car battery", "12V car battery", 8.1),
       ("12-pack drill bits", "12-pack of drill bits with sizes ranging from #40 to #3", 0.8),
       ("hammer", "12oz carpenter's hammer", 0.75),
       ("hammer", "14oz carpenter's hammer", 0.875),
       ("hammer", "16oz carpenter's hammer", 1.0),
       ("rocks", "box of assorted rocks", 5.3),
       ("jacket", "water resistent black wind breaker", 0.1),
       ("spare tire", "24 inch spare tire", 22.2),
       ("hammer", "12oz carpenter's hammer", 0.75),
       ("hammer", "14oz carpenter's hammer", 0.875),
       ("hammer", "16oz carpenter's hammer", 1.0),
       ("rocks", "box of assorted rocks", 5.3),
       ("jacket", "water resistent black wind breaker", 0.1),
       ("spare tire", "24 inch spare tire", 22.2);

-- Create and populate our users using a single insert with many rows
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);
INSERT INTO customers
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');