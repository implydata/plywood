/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

DROP TABLE IF EXISTS `wikipedia_raw`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `wikipedia_raw` (
  `time` datetime NOT NULL,
  `sometimeLater` timestamp NOT NULL,
  `channel` varchar(255) NOT NULL,
  `cityName` varchar(255) DEFAULT NULL,
  `comment` varchar(300) NOT NULL,
  `commentLength` int(11) NOT NULL,
  `countryIsoCode` varchar(255) DEFAULT NULL,
  `countryName` varchar(255) DEFAULT NULL,
  `isAnonymous` tinyint(1) NOT NULL,
  `isMinor` tinyint(1) NOT NULL,
  `isNew` tinyint(1) NOT NULL,
  `isRobot` tinyint(1) NOT NULL,
  `isUnpatrolled` tinyint(1) NOT NULL,
  `metroCode` int(11) DEFAULT NULL,
  `namespace` varchar(255) DEFAULT NULL,
  `page` varchar(255) DEFAULT NULL,
  `regionIsoCode` varchar(255) DEFAULT NULL,
  `regionName` varchar(255) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `delta` int(11) NOT NULL,
  `added` int(11) NOT NULL,
  `deleted` int(11) NOT NULL,
  `deltaByTen` float NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
