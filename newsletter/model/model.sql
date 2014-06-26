CREATE TABLE newsletter (
	id_newsletter int(6) NOT NULL AUTO_INCREMENT,
	header_text TEXT NOT NULL,
	banner_1_active TINYINT,
	banner_1_url VARCHAR(256),
	banner_1_image VARCHAR(256),
	banner_2_active TINYINT,
	banner_2_url VARCHAR(256),
	banner_2_image VARCHAR(256),
	banner_3_active TINYINT,
	banner_3_url VARCHAR(256),
	banner_3_image VARCHAR(256),
	banner_4_active TINYINT,
	banner_4_url VARCHAR(256),
	banner_4_image VARCHAR(256),
	type_link char(1) NOT NULL,
	id_affil INT(4),
	template char(1) NOT NULL,
	date_from DATE,
	date_to DATE,
	state TINYINT,
	PRIMARY KEY (id_newsletter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE category (
	category VARCHAR(64) NOT NULL,
	PRIMARY KEY (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	
CREATE TABLE newsletter_category (
	id_newsletter int(6) NOT NULL AUTO_INCREMENT,
	category VARCHAR(64) NOT NULL,
	PRIMARY KEY (id_newsletter, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

