SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
DROP DATABASE IF EXISTS `cc`;
CREATE DATABASE `cc`;
USE `cc`;
DROP TABLE IF EXISTS `hashTable`;
CREATE TABLE IF NOT EXISTS `hashTable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(50) NOT NULL,
  `site_id` int(11) NOT NULL,
  `position` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

DROP TABLE IF EXISTS `sites`;
CREATE TABLE IF NOT EXISTS `sites` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` text NOT NULL,
  `pagerank` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=4 ;
INSERT INTO `sites` (`id`, `name`, `pagerank`) VALUES
(1, 'http://web.ist.utl.pt/ist172526/cc/1.html', 1),
(2, 'http://web.ist.utl.pt/ist172526/cc/2.html', 1),
(3, 'http://web.ist.utl.pt/ist172526/cc/3.html', 1),
(4, 'http://web.ist.utl.pt/ist172528/cc/2lev1.html', 1);

CREATE INDEX word_index ON hashTable (word);
