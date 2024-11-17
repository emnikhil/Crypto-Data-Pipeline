CREATE OR REPLACE PROCEDURE `crypto-data-analysis-441505.source_codes.sp_market_cap_analysis`()
BEGIN
  DECLARE coins ARRAY<STRING> DEFAULT ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana'];
  DECLARE v_sql STRING;

  FOR coin_name IN (SELECT * FROM UNNEST(coins)) 
    DO
      -- Insert USD Market Cap
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.market_metrics.<table_name>_market_cap(
                      `date`, currency, total_market_cap
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'USD' AS currency,
                      SUM(market_cap) AS total_market_cap
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'USD'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;

      -- Insert EURO Market Cap
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.market_metrics.<table_name>_market_cap(
                      `date`, currency, total_market_cap
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'EUR' AS currency,
                      SUM(market_cap) AS total_market_cap
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'EUR'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;

      -- Insert INR Market Cap
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.market_metrics.<table_name>_market_cap(
                      `date`, currency, total_market_cap
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'INR' AS currency,
                      SUM(market_cap) AS total_market_cap
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'INR'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;
  END FOR;

  EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = ('Error in sp_market_cap_analysis ' || @@error.message);
END;