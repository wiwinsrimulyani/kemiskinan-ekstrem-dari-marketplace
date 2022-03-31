SELECT
  *,
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY kabupaten, cum_yearmonth_c ORDER BY c_goods DESC) AS rank
  FROM (
    SELECT
      * EXCEPT(TRUE_2 ),
      CASE cum_yearmonth_c
        WHEN 202001 THEN FORMAT_DATE("%b %Y", DATE "2020-01-01")
        WHEN 202002 THEN FORMAT_DATE("%b %Y", DATE "2020-02-01")
        WHEN 202003 THEN FORMAT_DATE("%b %Y", DATE "2020-03-01")
        WHEN 202004 THEN FORMAT_DATE("%b %Y", DATE "2020-04-01")
        WHEN 202005 THEN FORMAT_DATE("%b %Y", DATE "2020-05-01")
        WHEN 202006 THEN FORMAT_DATE("%b %Y", DATE "2020-06-01")
        WHEN 202007 THEN FORMAT_DATE("%b %Y", DATE "2020-07-01")
        WHEN 202008 THEN FORMAT_DATE("%b %Y", DATE "2020-08-01")
        WHEN 202009 THEN FORMAT_DATE("%b %Y", DATE "2020-09-01")
        WHEN 202010 THEN FORMAT_DATE("%b %Y", DATE "2020-10-01")
        WHEN 202011 THEN FORMAT_DATE("%b %Y", DATE "2020-11-01")
        WHEN 202012 THEN FORMAT_DATE("%b %Y", DATE "2020-12-01")
    END
      AS yyear_month,
      SUM(cum_shop_count) OVER(PARTITION BY kabupaten ORDER BY cum_yearmonth_c) AS running_total_shop
    FROM (
      SELECT
        *
      FROM (
        SELECT
          *,
          REGEXP_CONTAINS(kabupaten, 'KAB. BANDUNG BARAT')
          OR REGEXP_CONTAINS(kabupaten, 'KOTA BANDUNG')
          OR REGEXP_CONTAINS(kabupaten, 'KOTA PROBOLINGGO') AS TRUE_2,
        FROM (
          SELECT
            DISTINCT provinsi,
            kabupaten,
            cum_yearmonth_c,
            sub_sub_name,
            SUM(m_sold_adj_final) OVER(PARTITION BY kabupaten, cum_yearmonth_c) AS cum_sold,
            COUNT(DISTINCT shop_id_1) OVER(PARTITION BY kabupaten, cum_yearmonth_c) AS cum_shop_count,
            COUNT(sub_sub_name) OVER(PARTITION BY kabupaten, cum_yearmonth_c, sub_sub_name) AS c_goods
          FROM (
            SELECT
              *,
              CASE
                WHEN yearmonth_c <= 202001 THEN 202001
                WHEN yearmonth_c = 202002 THEN 202002
                WHEN yearmonth_c = 202003 THEN 202003
                WHEN yearmonth_c = 202004 THEN 202004
                WHEN yearmonth_c = 202005 THEN 202005
                WHEN yearmonth_c = 202006 THEN 202006
                WHEN yearmonth_c = 202007 THEN 202007
                WHEN yearmonth_c = 202008 THEN 202008
                WHEN yearmonth_c = 202009 THEN 202009
                WHEN yearmonth_c = 202010 THEN 202010
                WHEN yearmonth_c = 202011 THEN 202011
                WHEN yearmonth_c = 202012 THEN 202012
                WHEN yearmonth_c > 202012 THEN 0
            END
              AS cum_yearmonth_c
            FROM (
              SELECT
                *
              FROM (
                SELECT
                  *,
                  shop_id AS shop_id_1,
                  CAST(CONCAT(CAST(EXTRACT(YEAR
                        FROM
                          ctime) AS string), LPAD(CAST(EXTRACT(MONTH
                          FROM
                            ctime) AS string),
                        2,
                        '0') ) AS INT64)AS yearmonth_c
                FROM (
                  SELECT
                    DISTINCT * EXCEPT(shop_id_1 )
                  FROM (
                    SELECT
                      *
                    FROM (
                      SELECT
                        shop_id AS shop_id_1,
                        kabupaten,
                        provinsi,
                        ctime,
                        REGEXP_CONTAINS(kabupaten, 'CIANJUR')
                        OR REGEXP_CONTAINS(kabupaten, 'BANDUNG')
                        OR REGEXP_CONTAINS(kabupaten, 'KUNINGAN')
                        OR REGEXP_CONTAINS(kabupaten, 'INDRAMAYU')
                        OR REGEXP_CONTAINS(kabupaten, 'KARAWANG')
                        OR REGEXP_CONTAINS(kabupaten, 'BANYUMAS')
                        OR REGEXP_CONTAINS(kabupaten, 'BANJARNEGARA')
                        OR REGEXP_CONTAINS(kabupaten, 'KEBUMEN')
                        OR REGEXP_CONTAINS(kabupaten, 'PEMALANG')
                        OR REGEXP_CONTAINS(kabupaten, 'BREBES')
                        OR REGEXP_CONTAINS(kabupaten, 'PROBOLINGGO')
                        OR REGEXP_CONTAINS(kabupaten, 'BOJONEGORO')
                        OR REGEXP_CONTAINS(kabupaten, 'LAMONGAN')
                        OR REGEXP_CONTAINS(kabupaten, 'BANGKALAN')
                        OR REGEXP_CONTAINS(kabupaten, 'SUMENEP')
                        OR REGEXP_CONTAINS(kabupaten, 'SUMBA TIMUR')
                        OR REGEXP_CONTAINS(kabupaten, 'TIMOR TENGAH SELATAN')
                        OR REGEXP_CONTAINS(kabupaten, 'ROTE NDAO')
                        OR REGEXP_CONTAINS(kabupaten, 'SUMBA TENGAH')
                        OR REGEXP_CONTAINS(kabupaten, 'MANGGARAI TIMUR')
                        OR REGEXP_CONTAINS(kabupaten, 'MALUKU TENGGARA BARAT')
                        OR REGEXP_CONTAINS(kabupaten, 'MALUKU TENGGARA')
                        OR REGEXP_CONTAINS(kabupaten, 'MALUKU TENGAH')
                        OR REGEXP_CONTAINS(kabupaten, 'SERAM BAGIAN TIMUR')
                        OR REGEXP_CONTAINS(kabupaten, 'MALUKU BARAT DAYA')
                        OR REGEXP_CONTAINS(kabupaten, 'TELUK WONDAMA')
                        OR REGEXP_CONTAINS(kabupaten, 'TELUK BINTUNI')
                        OR REGEXP_CONTAINS(kabupaten, 'TAMBRAUW')
                        OR REGEXP_CONTAINS(kabupaten, 'MAYBRAT')
                        OR REGEXP_CONTAINS(kabupaten, 'MANOKWARI SELATAN')
                        OR REGEXP_CONTAINS(kabupaten, 'JAYAWIJAYA')
                        OR REGEXP_CONTAINS(kabupaten, 'PUNCAK JAYA')
                        OR REGEXP_CONTAINS(kabupaten, 'LANNY JAYA')
                        OR REGEXP_CONTAINS(kabupaten, 'MAMBERAMO TENGAH')
                        OR REGEXP_CONTAINS(kabupaten, 'DEIYAI') AS val_true_kab
                      FROM
                        `my-2nd-skripsi.diskusi_19_Mei_2021.shop_merged_all`)
                    WHERE
                      val_true_kab = TRUE ) AS a
                  LEFT JOIN (
                    SELECT
                      shop_id,
                      parent_name,
                      sub_name,
                      sub_sub_name,
                      yearmonth,
                      m_sold_adj_final,
                      CASE yearmonth
                        WHEN '202001' THEN FORMAT_DATE("%b %Y", DATE "2020-01-01")
                        WHEN '202002' THEN FORMAT_DATE("%b %Y", DATE "2020-02-01")
                        WHEN '202003' THEN FORMAT_DATE("%b %Y", DATE "2020-03-01")
                        WHEN '202004' THEN FORMAT_DATE("%b %Y", DATE "2020-04-01")
                        WHEN '202005' THEN FORMAT_DATE("%b %Y", DATE "2020-05-01")
                        WHEN '202006' THEN FORMAT_DATE("%b %Y", DATE "2020-06-01")
                        WHEN '202007' THEN FORMAT_DATE("%b %Y", DATE "2020-07-01")
                        WHEN '202008' THEN FORMAT_DATE("%b %Y", DATE "2020-08-01")
                        WHEN '202009' THEN FORMAT_DATE("%b %Y", DATE "2020-09-01")
                        WHEN '202010' THEN FORMAT_DATE("%b %Y", DATE "2020-10-01")
                        WHEN '202011' THEN FORMAT_DATE("%b %Y", DATE "2020-11-01")
                        WHEN '202012' THEN FORMAT_DATE("%b %Y", DATE "2020-12-01")
                    END
                      AS year_month,
                    FROM
                      `my-2nd-skripsi.try_keseluruhan.im_aug`
                    WHERE
                      yearmonth = '202001'
                      OR yearmonth = '202002'
                      OR yearmonth = '202003'
                      OR yearmonth = '202004'
                      OR yearmonth = '202005'
                      OR yearmonth = '202006'
                      OR yearmonth = '202007'
                      OR yearmonth = '202008'
                      OR yearmonth = '202009'
                      OR yearmonth = '202010'
                      OR yearmonth = '202011'
                      OR yearmonth = '202012') AS b
                  ON
                    a.shop_id_1 = b.shop_id
                  WHERE
                    shop_id IS NOT NULL)) ))
          ORDER BY
            provinsi,
            kabupaten,
            cum_yearmonth_c)
        WHERE
          cum_yearmonth_c != 0)
      WHERE
        true_2 IS FALSE)) )
WHERE
  rank <= 3
ORDER BY
  provinsi ASC,
  kabupaten ASC,
  cum_yearmonth_c ASC,
  c_goods DESC