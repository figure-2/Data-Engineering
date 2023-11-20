-- 01. 주요 시/도 연령대별 인구수 합계 구하기

SELECT 
	A.AGRDE_SE_CD,
    SUM(A.POPLTN_CNT) AS AGRDE_POPLTN_CNT
FROM TB_POPLTN A
WHERE A.ADMINIST_ZONE_NO LIKE '__00000000' -- 어차피 시/도 코드는 앞의 2자리가다르고, 나머지 8자리는 0
AND A.STD_MT = '202304' -- 날짜 같이 조회하는 경우
AND A.POPLTN_SE_CD = 'T' -- 남자 여자 따로 보지 않음
GROUP BY A.AGRDE_SE_CD
ORDER BY A.AGRDE_SE_CD;

SELECT *
FROM TB_POPLTN;


-- 2023년 4월 기준 전국 주요 시/도의 연령대별 인구수 합계를 구하고, 연령대별 인구 비율 구하기

-- SELECT
-- 	B.AGRDE_SE_CD, -- 데이터가 N개
--     B.AGRDE_POPLTN_CNT, -- 데이터가 N개
--     SUM(B.AGRDE_POPLTN_CNT) OVER() AS SUM_AGRFE_POPLTN_CNT -- -- 데이터가 1개
--     -- 조회해야될 행이 2개는 N개 이고 하나는 1개 이기 때문에 조회해야되는 행의 갯수가 안 맞기 때문에 에러가 발생한다.
--     -- group by를 해도 B.AGRDE_POPLTN_CNT와 같은 결과 값이 나온다.
--     -- group by는 SELECT를 하기 전에 집계를 한다
--     -- OVER 라는 함수는 SELECT를 하고 나서 집계를 할 수 있도록 도와주는것
--     -- OVER()는 집계 기준이 없기 때문에 N개의 DATA에 대해 집계를 한다. SELECT된 전체 대상해 대한 집계

-- -- 위에서 만들 커리를 서브커리로 만들어서 사용한다 ( 연령대, 연령대 인구수, 전체 인구가 필요하다)
-- FROM (
-- 		SELECT
-- 			A.AGRDE_SE_CD,
-- 			SUM(A.POPLTN_CNT) AS AGRDE_POPLTN_CNT
-- 			
-- 		FROM TB_POPLTN A

-- 		WHERE A.ADMIMST_ZONE_NO LIKE '__00000000' -- 시/도 코드는 앞의 2자리가 다르고, 나머지 8자리는 0
-- 			AND A.STD_MT = '202304'
-- 			AND A.POPLTN_SE_CD = 'T'
-- 		GROUP BY A.AGRDE_SE_CD
-- 		ORDER BY A.AGRDE_SE_CD
-- ) B

-- 위에 커리롤 바탕으로 하기 때문에 또 서브 커리를 만든다

SELECT 
	C.AGRDE_SE_CD,
    C.AGRDE_POPLTN_CNT,
    CONCAT(ROUND((AGRDE_POPLTN_CNT/SUM_AGRDE_POPLTN_CNT) * 100, 2), '%') AS "인구비율" -- 문자열 붙일 수 있음.
        -- CONCAT을 사용하면 뒤에 원하는 문자열을 넣을 수 있다  '%') 

FROM(SELECT 
	B.AGRDE_SE_CD,
    B.AGRDE_POPLTN_CNT,
    SUM(AGRDE_POPLTN_CNT) OVER() AS SUM_AGRDE_POPLTN_CNT
FROM (SELECT 
		A.AGRDE_SE_CD,
		SUM(A.POPLTN_CNT) AS AGRDE_POPLTN_CNT
	FROM TB_POPLTN A
	WHERE A.ADMINIST_ZONE_NO LIKE '__00000000' -- 어차피 시/도 코드는 앞의 2자리가다르고, 나머지 8자리는 0
	AND A.STD_MT = '202304' -- 날짜 같이 조회하는 경우
	AND A.POPLTN_SE_CD = 'T' -- 남자 여자 따로 보지 않음
	GROUP BY A.AGRDE_SE_CD
	ORDER BY A.AGRDE_SE_CD
	) B
) C;

-- 2023년 04월 기준 전국의 성별 인구수 합계를 구하고, 남성/ 여성 비율 구하기
---- 해설
SELECT
    C.MALE_POPLTN_CNT,
    C.FEMALE_POPLTN_CNT,
    C.MALE_POPLTN_CNT / C.FEMALE_POPLTN_CNT AS "남성 / 여성 비율",
    C.MALE_POPLTN_CNT / (C.MALE_POPLTN_CNT+C.FEMALE_POPLTN_CNT) AS "전체 인구 대비 남성 비율",
    C.FEMALE_POPLTN_CNT / (C.MALE_POPLTN_CNT + C.FEMALE_POPLTN_CNT) AS "전체 인구 대비 여성 비율"
FROM (
        SELECT
            MAX(B.MALE_POPLTN_CNT) AS MALE_POPLTN_CNT,
            MAX(B.FEMALE_POPLTN_CNT) AS FEMALE_POPLTN_CNT
        FROM (
                SELECT
                    A.POPLTN_SE_CD,
                    CASE WHEN A.POPLTN_SE_CD = 'M' THEN SUM(A.POPLTN_CNT) ELSE 0 END MALE_POPLTN_CNT,
                    CASE WHEN A.POPLTN_SE_CD = 'F' THEN SUM(A.POPLTN_CNT) ELSE 0 END FEMALE_POPLTN_CNT
                FROM TB_POPLTN A
                WHERE A.ADMINIST_ZONE_NO LIKE '__00000000'
                  AND A.POPLTN_SE_CD IN ('M', 'F')
                  AND A.STD_MT = '202304'
                GROUP BY A.POPLTN_SE_CD
                ORDER BY A.POPLTN_SE_CD
             ) B
    ) C;



-- ---
-- SELECT
--     POPLTN_SE_CD,
--     SUM(POPLTN_CNT) AS TOTAL_POPLTN_CNT,
--     CONCAT(ROUND((SUM(POPLTN_CNT) / (SELECT SUM(POPLTN_CNT) FROM TB_POPLTN WHERE STD_MT = '202304')) * 100, 2), '%') AS '비율'
-- FROM TB_POPLTN
-- WHERE STD_MT = '202304'
-- GROUP BY POPLTN_SE_CD;
-- -----
-- SELECT 
--     A.POPLTN_SE_CD,
--     SUM(A.POPLTN_CNT) AS TOTAL_POPLTN_CNT,
--     CONCAT(ROUND((SUM(A.POPLTN_CNT) / B.TOTAL_POPULATION) * 100, 2), '%') AS 'POPULATION_RATIO'
-- FROM 
--     TB_POPLTN A,
--     (SELECT SUM(POPLTN_CNT) AS TOTAL_POPULATION FROM TB_POPLTN WHERE STD_MT = '202304' AND POPLTN_SE_CD <> 'T') B
-- WHERE 
--     A.STD_MT = '202304'
--     AND A.POPLTN_SE_CD <> 'T'
-- GROUP BY 
--     A.POPLTN_SE_CD;



-- 남자 인구수, 여자 인구수, 남자 : 여자 비율 ( 남자 인구 / 여자 인구 ), 남자 비율 (남자 / 전체) , 여자 비율 (여자 / 전체)

-- 2번 문제: 남자 인구수, 여자 인구수, 남자:여자 비율, 남자 비율, 여자 비율
-- SELECT
--     SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT ELSE 0 END) AS "남자 인구수",
--     SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END) AS "여자 인구수",
--     CONCAT(ROUND(SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT END) / SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT END), 2), ':1') AS "남자:여자 비율",
--     CONCAT(ROUND(SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT END) / SUM(A.POPLTN_CNT), 4)) AS "남자 비율",
--     CONCAT(ROUND(SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT END) / SUM(A.POPLTN_CNT), 4)) AS "여자 비율"
-- FROM TB_POPLTN A
-- WHERE A.STD_MT = '202304'; -- 2023년 04월



-- OVER() 안써도 됨, CASE WHEN THEN 사용할 것, MAX 사용함.TB_PBTRNSP
-- 기초 데이터를 먼저 조회를 하고 데이터를 변형을 하면서 결과값을 찾는다.

-- 연령대 별 인구가 가장 많은 지역 구하가
-- 2023년 04월 기준 전국의 읍/면/동의 인구수 (남 + 여) 를 조회alter

SELECT
    A.AGRDE_SE_CD,
    A.ADMINIST_ZONE_NO,
    A.ADMINIST_ZONE_NM,
    A.POPLTN_CNT
FROM TB_POPLTN A
WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
  AND A.POPLTN_SE_CD = 'T'
  AND A.POPLTN_CNT > 0
  AND A.STD_MT = '202304'
ORDER BY POPLTN_CNT DESC;



-- 카테고리를 만들어서 그 안에서만 정렬을 진행
-- 연령대 별로 인구수가 가장 많은 순으로 정렬하기
-- 000 지역이름1 인구수
-- 000 지역이름2 인구수
-- 000 지역이름3 인구수

SELECT
    B.AGRDE_SE_CD,
    B.ADMINIST_ZONE_NO,
    B.ADMINIST_ZONE_NM,
    B.POPLTN_CNT,
    ROW_NUMBER() OVER (PARTITION BY B.AGRDE_SE_CD ORDER BY B.POPLTN_CNT DESC) AS RNUM
FROM (
        SELECT
            A.AGRDE_SE_CD,
            A.ADMINIST_ZONE_NO,
            A.ADMINIST_ZONE_NM,
            A.POPLTN_CNT
        FROM TB_POPLTN A
        WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
          AND A.POPLTN_SE_CD = 'T'
          AND A.POPLTN_CNT > 0
          AND A.STD_MT = '202304'
        ORDER BY POPLTN_CNT DESC
     ) B;


SELECT
    C.AGRDE_SE_CD, C.ADMINIST_ZONE_NO, C.ADMINIST_ZONE_NM, C.POPLTN_CNT
FROM (
        SELECT
            B.AGRDE_SE_CD,
            B.ADMINIST_ZONE_NO,
            B.ADMINIST_ZONE_NM,
            B.POPLTN_CNT,
            ROW_NUMBER() OVER (PARTITION BY B.AGRDE_SE_CD ORDER BY B.POPLTN_CNT DESC) AS RNUM
        FROM (
                SELECT
                    A.AGRDE_SE_CD,
                    A.ADMINIST_ZONE_NO,
                    A.ADMINIST_ZONE_NM,
                    A.POPLTN_CNT
                FROM TB_POPLTN A
                WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
                  AND A.POPLTN_SE_CD = 'T'
                  AND A.POPLTN_CNT > 0
                  AND A.STD_MT = '202304'
                ORDER BY POPLTN_CNT DESC
             ) B
     ) C
WHERE C.RNUM = 1 -- 각 파티션별 랭킹 1위만 가져오기
ORDER BY C.AGRDE_SE_CD;


-- 1) 연령대별 인구비율이 가장 높은 지역 구하기

-- 2023년 04월 기준 전국의 각 지역의 연령대별 인구수 비율이 높은 지역을 찾는다.
-- POPLTN_CNT / SUM(POPLTN_CNT) OVER(PARTITION BY ADMINIST_ZONE_NO)
-- 성별 코드가 'T'인 데이터 대상으로.
-- ADMINIST_ZONE_NO NOT LIKE '_____00000'

-- 2023년 04월 기준 전국의 각 지역의 연령대별 인구수 비율 구하기

-- 해설
-- 전체 데이터 대상에서 집계는 내가 정한 일부의 기준에서만 집계를 한다 이때 사용하는데 => OVER()
SELECT
    A.AGRDE_SE_CD,
    A.ADMINIST_ZONE_NO
    A.ADMINIST_ZONE)NM,
    A.POPLTN_CNT

FROM TB_POPLTN A
WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
  AND A.POPLTN_SE_CD = 'T'
  AND A.POPLTN_CNT > 0
  AND A.STD_MT = '202304'
ORDER BY A.POLTN_CNT DESC;


--

SELECT
    B.AGRDE_SE_CD,
    B.ADMINIST_ZONE_NO,
    B.ADMINIST_ZONE_NM,
    B.POPLTN_CNT,
    (B.POPLTN_CNT / SUM(B.POPLTN_CNT) OVER(PARTITION BY B.ADMINIST_ZONE_NO)) AS `행정구역별 각 연령대별의 인구비율`
FROM (
    SELECT
        A.AGRDE_SE_CD,
        A.ADMINIST_ZONE_NO,
        A.ADMINIST_ZONE_NM,
        A.POPLTN_CNT
    FROM TB_POPLTN A
    WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
    AND A.POPLTN_SE_CD = 'T'
    AND A.POPLTN_CNT > 0
    AND A.STD_MT = '202304'
    ORDER BY A.POPLTN_CNT DESC
) B;


-- 연령대별 인구수 / 행정구역별 전체 인구수
SELECT *, (POPLTN_CNT / SUM(POPLTN_CNT)) OVER(PARTITION BY ADMINIST_ZONE_NO) -- 1개짜리 데이터를 11개로 펼친다
FROM TB_POPLTN WHERE ADMINIST_ZONE_NO = 4119060600 AND POPLTN_SE_CD = 'T'


--  연습
SELECT 
    D.AGRDE_SE_CD, 
    D.ADMINIST_ZONE_NO, 
    D.ADMINIST_ZONE_NM, 
    D.POPLTN_CNT / D.SUM_POPLTN_CNT AS "인구 비율"
FROM (
    SELECT 
        C.AGRDE_SE_CD, 
        C.ADMINIST_ZONE_NO, 
        C.ADMINIST_ZONE_NM, 
        C.POPLTN_CNT, 
        SUM(C.POPLTN_CNT) OVER (PARTITION BY C.ADMINIST_ZONE_NO) AS SUM_POPLTN_CNT
    FROM (
        SELECT 
            A.AGRDE_SE_CD, 
            A.ADMINIST_ZONE_NO, 
            A.ADMINIST_ZONE_NM, 
            A.POPLTN_CNT
        FROM TB_POPLTN A
        WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
          AND A.POPLTN_SE_CD = 'T'
          AND A.POPLTN_CNT > 0
          AND A.STD_MT = '202304'
    ) C
) D
ORDER BY D.AGRDE_SE_CD, D."인구 비율" DESC;



-- 2) 남성보다 여성의 수가 가장 많은 지역 구하기
-- 전국의 각 읍/면/동 기준 남성의 수보다 여성의 수가 가장 많은 지역을 구한다.
-- 여성인구수 - 남성인구수

-- SELECT
--     A.ADMINIST_ZONE_NO,
--     A.ADMINIST_ZONE_NM,
--     SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT ELSE 0 END) AS MALE_POPLTN_CNT,
--     SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END) AS FEMALE_POPLTN_CNT
-- FROM TB_POPLTN A
-- WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
--   AND A.POPLTN_SE_CD IN ('M', 'F')
--   AND A.STD_MT = '202304'
-- GROUP BY A.ADMINIST_ZONE_NO, A.ADMINIST_ZONE_NM
-- HAVING SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT ELSE 0 END) < SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END);

SELECT
 B.ADMINIST_ZONE_NO, 
 B.ADMINIST_ZONE_NM, 
 B.MALE_POPLTN_CNT, 
 B.FEMALE_POPLTN_CNT
FROM (
    SELECT
        A.ADMINIST_ZONE_NO,
        A.ADMINIST_ZONE_NM,
        SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT ELSE 0 END) AS MALE_POPLTN_CNT,
        SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END) AS FEMALE_POPLTN_CNT,
        RANK() OVER (ORDER BY SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END) DESC) AS RANK_NUM
    FROM TB_POPLTN A
    WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
      AND A.POPLTN_SE_CD IN ('M', 'F')
      AND A.STD_MT = '202304'
    GROUP BY A.ADMINIST_ZONE_NO, A.ADMINIST_ZONE_NM
    HAVING SUM(CASE WHEN A.POPLTN_SE_CD = 'M' THEN A.POPLTN_CNT ELSE 0 END) < SUM(CASE WHEN A.POPLTN_SE_CD = 'F' THEN A.POPLTN_CNT ELSE 0 END)
) B
WHERE B.RANK_NUM = 1;

-- 랭킹을 할때 는 over()필요한다. 또한 전체 데이터를 볼꺼면 굳이 파티션을 나눌 필요가 없다.


-- 3) 남성/여성 비율이 가장 높은 지역과 가장 낮은 지역 구하기
-- 전국의 각 읍/면/동 기준 남성 비율 및 여성 비율이 가장 높거나 낮은 지역 구하기

-- 1) 연령대별 인구비율이 가장 높은 지역 구하기

-- 2023년 04월 기준 전국의 각 지역의 연령대별 인구수 비율이 높은 지역을 찾는다. GROUP BY 지역코드, 연령대를 토대로 최곳값을 뽑으면 각 지역에 비율이 가장 큰 데이터가 나온다.
-- POPLTN_CNT / SUM(POPLTN_CNT) OVER(PARTITION BY ADMINIST_ZONE_NO)
-- 성별 코드가 'T'인 데이터 대상으로.
-- ADMINIST_ZONE_NO NOT LIKE '_____00000'
SELECT
    A.AGRDE_SE_CD,
    A.ADMINIST_ZONE_NO,
    A.ADMINIST_ZONE_NM,
    A.POPLTN_CNT
FROM TB_POPLTN A
WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
  AND A.POPLTN_SE_CD = 'T'
  AND A.POPLTN_CNT > 0
  AND A.STD_MT = '202304'
ORDER BY A.POPLTN_CNT DESC;

SELECT
    D.AGRDE_SE_CD, ADMINIST_ZONE_NO, ADMINIST_ZONE_NM, POPLTN_CNT, 행정구역별각연령대의인구비율
FROM (SELECT C.AGRDE_SE_CD,
             C.ADMINIST_ZONE_NO,
             C.ADMINIST_ZONE_NM,
             C.POPLTN_CNT,
             C.행정구역별각연령대의인구비율,
             ROW_NUMBER() OVER (PARTITION BY C.AGRDE_SE_CD ORDER BY C.행정구역별각연령대의인구비율 DESC) AS RNUM
      FROM (SELECT B.AGRDE_SE_CD,
                   B.ADMINIST_ZONE_NO,
                   B.ADMINIST_ZONE_NM,
                   B.POPLTN_CNT,
                   (B.POPLTN_CNT / SUM(B.POPLTN_CNT) OVER (PARTITION BY B.ADMINIST_ZONE_NO)) AS '행정구역별각연령대의인구비율'
            FROM (SELECT A.AGRDE_SE_CD,
                         A.ADMINIST_ZONE_NO,
                         A.ADMINIST_ZONE_NM,
                         A.POPLTN_CNT
                  FROM TB_POPLTN A
                  WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
                    AND A.POPLTN_SE_CD = 'T'
                    AND A.POPLTN_CNT > 0
                    AND A.STD_MT = '202304'
                  ORDER BY A.POPLTN_CNT DESC) B) C ) D
WHERE D.RNUM = 1;
-- 2) 남성보다 여성의 수가 가장 많은 지역 구하기
-- 전국의 각 읍/면/동 기준 남성의 수보다 여성의 수가 가장 많은 지역을 구한다.
-- 여성인구수 - 남성인구수
SELECT
    A.ADMINIST_ZONE_NO,
    A.ADMINIST_ZONE_NM,
    A.POPLTN_SE_CD,
    SUM(A.POPLTN_CNT) AS POPLTN_CNT
FROM TB_POPLTN A
WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
  AND A.POPLTN_SE_CD IN ('M', 'F')
  AND A.STD_MT = '202304'
GROUP BY A.ADMINIST_ZONE_NO, A.ADMINIST_ZONE_NM, A.POPLTN_SE_CD
ORDER BY A.ADMINIST_ZONE_NO, A.POPLTN_SE_CD;

SELECT
    D.ADMINIST_ZONE_NO,
    D.ADMINIST_ZONE_NM,
    D.FEMALE_POPLTN_CNT - D.MALE_POPLTN_CNT AS "FEMALE_MALE_DIFF"
FROM (
          SELECT C.ADMINIST_ZONE_NO,
                 C.ADMINIST_ZONE_NM,
                 MAX(C.MALE_POPLTN_CNT)   AS MALE_POPLTN_CNT,
                 MAX(C.FEMALE_POPLTN_CNT) AS FEMALE_POPLTN_CNT
          FROM (SELECT B.ADMINIST_ZONE_NO,
                       B.ADMINIST_ZONE_NM,
                       CASE WHEN B.POPLTN_SE_CD = 'M' THEN B.POPLTN_CNT ELSE 0 END AS MALE_POPLTN_CNT,
                       CASE WHEN B.POPLTN_SE_CD = 'F' THEN B.POPLTN_CNT ELSE 0 END AS FEMALE_POPLTN_CNT
                FROM (SELECT A.ADMINIST_ZONE_NO,
                             A.ADMINIST_ZONE_NM,
                             A.POPLTN_SE_CD,
                             SUM(A.POPLTN_CNT) AS POPLTN_CNT
                      FROM TB_POPLTN A
                      WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
                        AND A.POPLTN_SE_CD IN ('M', 'F')
                        AND A.STD_MT = '202304'
                      GROUP BY A.ADMINIST_ZONE_NO, A.ADMINIST_ZONE_NM, A.POPLTN_SE_CD
                      ORDER BY A.ADMINIST_ZONE_NO, A.POPLTN_SE_CD) B
                ) C
          GROUP BY C.ADMINIST_ZONE_NO, C.ADMINIST_ZONE_NM
      ) D
ORDER BY D.FEMALE_POPLTN_CNT - D.MALE_POPLTN_CNT DESC
LIMIT 1;

-- 3) 남성/여성 비율이 가장 높은 지역과 가장 낮은 지역 구하기
-- 전국의 각 읍/면/동 기준 남성 비율 및 여성 비율이 가장 높거나 낮은 지역 구하기.
SELECT
    *
FROM (
        SELECT
            D.ADMINIST_ZONE_NO,
            D.ADMINIST_ZONE_NM,
            D.MALE_POPLTN_CNT,
            D.FEMALE_POPLTN_CNT,
            D.MALE_POPLTN_CNT / D.TOT_POPLTN_CNT AS "남성인구비율",
            D.FEMALE_POPLTN_CNT / D.TOT_POPLTN_CNT AS "여성인구비율",
            ROW_NUMBER() OVER (ORDER BY D.MALE_POPLTN_CNT / D.TOT_POPLTN_CNT) AS MALE_RATE_ASC,
            ROW_NUMBER() OVER (ORDER BY D.FEMALE_POPLTN_CNT / D.TOT_POPLTN_CNT) AS FEMALE_RATE_ASC,
            ROW_NUMBER() OVER (ORDER BY D.MALE_POPLTN_CNT / D.TOT_POPLTN_CNT DESC) AS MALE_RATE_DESC,
            ROW_NUMBER() OVER (ORDER BY D.FEMALE_POPLTN_CNT / D.TOT_POPLTN_CNT DESC) AS FEMALE_RATE_DESC
        FROM (SELECT C.ADMINIST_ZONE_NO,
                     C.ADMINIST_ZONE_NM,
                     MAX(C.MALE_POPLTN_CNT)                            AS MALE_POPLTN_CNT,
                     MAX(C.FEMALE_POPLTN_CNT)                          AS FEMALE_POPLTN_CNT,
                     MAX(C.MALE_POPLTN_CNT) + MAX(C.FEMALE_POPLTN_CNT) AS TOT_POPLTN_CNT
              FROM (SELECT B.ADMINIST_ZONE_NO,
                           B.ADMINIST_ZONE_NM,
                           CASE WHEN B.POPLTN_SE_CD = 'M' THEN B.POPLTN_CNT ELSE 0 END AS MALE_POPLTN_CNT,
                           CASE WHEN B.POPLTN_SE_CD = 'F' THEN B.POPLTN_CNT ELSE 0 END AS FEMALE_POPLTN_CNT
                    FROM (SELECT A.ADMINIST_ZONE_NO,
                                 A.ADMINIST_ZONE_NM,
                                 A.POPLTN_SE_CD,
                                 SUM(A.POPLTN_CNT) AS POPLTN_CNT
                          FROM TB_POPLTN A
                          WHERE A.ADMINIST_ZONE_NO NOT LIKE '_____00000'
                            AND A.POPLTN_SE_CD IN ('M', 'F')
                            AND A.STD_MT = '202304'
                            AND A.POPLTN_CNT > 0
                          GROUP BY A.ADMINIST_ZONE_NO, A.ADMINIST_ZONE_NM, A.POPLTN_SE_CD
                          ORDER BY A.ADMINIST_ZONE_NO, A.POPLTN_SE_CD) B ) C
              GROUP BY C.ADMINIST_ZONE_NO, C.ADMINIST_ZONE_NM) D
     ) E
WHERE E.MALE_RATE_ASC = 1 OR E.FEMALE_RATE_ASC = 1 OR E.MALE_RATE_DESC = 1 OR E.FEMALE_RATE_DESC = 1;