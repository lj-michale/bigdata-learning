


--创建BEIMU数据表
CREATE TABLE IF NOT EXISTS student(
    `id` INT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(20)  NULL ,
    `gender` VARCHAR(10) NULL,
    `age` INT,
    PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

--插入数据（暂时未学到可忽略，这章主要学习数创建据表）
INSERT INTO student(`id`,`name`,`gender`,`age`) VALUES (1,'张山', '男', 20);

