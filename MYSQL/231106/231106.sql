SELECT CountryCode, avg(Population) as "avg_pop"
FROM city
GROUP BY CountryCode; 

SELECT COUNTRYCODE, DISTRICT, count(*)
FROM city
GROUP BY COUNTRYCODE, DISTRICT;

SELECT COUNTRYCODE,
		AVG(POPULATION) AS " AVG_POP",
        STDDEV(POPULATION) AS "STD_POP",
        MAX(POPULATION) AS "MAX_POP",
        MIN(POPULATION) AS "MIN_POP"
FROM city
GROUP BY COUNTRYCODE
ORDER BY STD_POP DESC;

-- 인구수가 500만 이상인 도시에 대해 집계
SELECT COUNTRYCODE,
		AVG(POPULATION) AS " AVG_POP",
        STDDEV(POPULATION) AS "STD_POP",
        MAX(POPULATION) AS "MAX_POP",
        MIN(POPULATION) AS "MIN_POP"
FROM city
WHERE POPULATION >= 5000000
GROUP BY COUNTRYCODE
HAVING STD_POP >= 300000
ORDER BY STD_POP DESC;

-- 나라코드에 K가 들어가는 나라들의 인구수의 총 합이 5백만 이상인 나라만 조회(CountryCode, 인구수 합)
SELECT COUNTRYCODE, SUM(POPULATION) 'SUM_POP'
FROM city
WHERE COUNTRYCODE LIKE '%K%'
GROUP BY COUNTRYCODE
HAVING SUM_POP >= 5000000;

SELECT * FROM country;

-- 1. country 테이블에서 대륙별 인구수와 gnp 최대값을 조회
SELECT Continent, SUM(population), max(gnp)
FROM country
GROUP BY Continent;

-- 2. country 테이블에서 대륙(continent)별 인구수와 GNP 평균 값을 조회. (단 GNP와 인구수가 0이 아닌 데이터 중에서)
SELECT Continent, SUM(population), avg(gnp)
FROM country
WHERE GNP != 0 AND POPULATION != 0 
GROUP BY Continent;

-- 3. country 테이블에서 대륙별 평균 인구수와 평균 GNP 결과를 인구수로 내림차순 정렬( 단 GNP, Population 이 0이 아닌 나라)
SELECT Continent, avg(population) AVG_POP, avg(gnp)
FROM country
WHERE GNP != 0 AND POPULATION != 0 
GROUP BY Continent
ORDER BY AVG_POP DESC;

-- 4 country 테이블에서 대륙별 평균 인구수, 평균 GNP, 1인당 GNP(평균 GNP / 평균 인구 * 1000)
-- 결과를 1인당 GNP가 0.01 이상인 데이터를 조회하고 1인당 GNP를 내림차순으로 정렬( 단 gnp, population은 0이 아닌 나라)
SELECT Continent, avg(population) AVG_POP, avg(gnp) avg_gnp, avg(gnp)/avg(population)*1000 GNP_POP_AVG
FROM country
WHERE GNP != 0 AND POPULATION != 0 
GROUP BY Continent
HAVING GNP_POP_AVG >= 0.01
ORDER BY GNP_POP_AVG DESC;

-- SELECT절 문법
-- CEIL, ROUND, TRUNCATE
-- CEIL : 실수 데이터에서 올림 할 때 사용
SELECT CEIL(12.345);  

-- ROUND : 실수 데이터에서 반올림 할 때 사용
SELECT ROUND(12.345,2), ROUND(12.343, 2);

-- 국가 별 언어 사용 비율을 소수 첫 번째 자리에서 반올림하여 정수로 표현
SELECT COUNTRYCODE, LANGUAGE, ISOFFICIAL, ROUND(PERCENTAGE,0) FROM countrylanguage;

-- truncate : 실수 데이터를 버림(내림)할 때 사용
SELECT truncate(12.345, 2);

-- 조건문(Conditional)
-- IF(조건, 참 expr, 거짓 expr)
-- SELECT IF(10>11, "참입니다", "거짓입니다");
-- CASE WHEN : IF 의 상위 호환

-- CASE
-- 	WHEN (조건1) THEN expr
-- 	WHEN (조건2) THEN expr
-- 	WHEN (조건3) THEN expr
--     ELSE expr
-- END

SELECT Name, CASE
				WHEN POPULATION >= 1000000 THEN "Big City"
				WHEN POPULATION BETWEEN 500000 AND 999999 THEN "Middle City"
				ELSE "Small City"
            END CITY_SCALE
FROM city;


-- DATE_FORMAT
-- 날짜 데이터에 대한 포맷 변경
-- 날짜 형식으로 되어있는 데이터에서 필요한 부분만 추출하는 것이 가능
-- EX) 년도, 월, 일, 시간, 분, 초 식으로 추출이 가능

USE sakila;

SELECT * FROM payment;

SELECT payment_date, 
	   DATE_FORMAT(payment_date,"%Y") as "year",
       DATE_FORMAT(payment_date,"%m") as "month",
       DATE_FORMAT(payment_date,"%d") as "day",
       DATE_FORMAT(payment_date,"%Y년 %m월 %d일")
FROM payment;


-- JOIN
-- Cartesian Join

-- -- 클라이언트에게 서비스 할 때는 사용되지 않는다.
-- -- 데이터 분석에서는 Cartesian Join을 굉장히 많이 사용한다. Code 부여(집계용 코드)를 위해
-- SELECT * FROM user, addr;

-- inner join
SELECT *
FROM user -- FROM절에 오는 테이블이 기준 테이블
JOIN addr ON user.user_id = addr.user_id; -- 대상 테이블


-- left join
-- 모든 사용자의 이름과 주소를 출력
-- 주소가 없는 사람은 "주소없음"으로 출력
SELECT user.name, IFNULL(addr.addr,"주소없음")
FROM user
LEFT JOIN addr ON user.user_id = addr.user_id;

-- right join 거의 안씀


-- 서브쿼리
-- SELECT절

-- 1. SELECT 절에 SUB QUERY 사용하기, 결과물이 컬럼 1개 ROW 1개 

# 전체 나라 수, 전체 도시 수, 전체 언어 수
SELECT
    (SELECT COUNT(*) FROM city ) AS total_city_cnt,
    (SELECT COUNT(*) FROM country) AS total_country_cnt,
    (SELECT COUNT(*) FROM countrylanguage ) as total_language_cnt;

    
-- 도시 이름에 k가 들어가는 도시의 개수, 나라 이름에 A가 들어가는 나라들의 gnp 평균
 
use world;

SELECT 
    (SELECT COUNT(*) FROM city where name like '%K%') CNT, 
    (SELECT AVG(gnp) FROM country where name like '%A%') AVG_GNP;

-- FROM절에 SUB QUERY 사용하기 조회된 결과물을 테이블처럼 사용
SELECT COUNTRYCODE, NAME, POPULATION
   FROM city
   WHERE POPULATION >= 8000000

-- 위 쿼리에서 국가 이름 까지 출력하고 싶으면?
-- country 테이블과 join을 한다

-- 1. 서브쿼리를 사용하지 않고 구하기
SELECT b.code, b.Name, a.Name, a.Population
    FROM city a
    JOIN country b ON a.CountryCode = b.Code
    WHERE a.Population >= 8000000;

-- 2. 서브쿼리를 사용하고 구하기

SELECT city_sub.CountryCode,
	   city_sub.Name as "city_name",
       city_sub.Population as "city_pop",
       country.Name as "country_name"
FROM ( SELECT COUNTRYCODE, NAME, POPULATION
	   FROM city
       WHERE POPULATION >= 8000000
	 ) AS city_sub
JOIN country ON city_sub.CountryCode = country.Code;


-- WHERE절 대부분 in연산을 할따 사용
SELECT *
    FROM country
    WHERE Code IN ('kor', 'jpn', 'usa');

-- 도시 인구수 800만 이상 도시의 국가코드, 국가 이름, 대통령 이름 출력
SELECT DISTRICT(CountryCode)
    FROM city
    WHERE Population >= 8000000;


SELECT code, NAME, headofstate
FROM country
WHERE CODE IN ( SELECT distinct(CountryCode)
				FROM city
				WHERE POPULATION >= 8000000 );
