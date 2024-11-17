CREATE OR REPLACE PROCEDURE `crypto-data-analysis-441505.source_codes.sp_24hr_volume_analysis`()
BEGIN
  DECLARE coins ARRAY<STRING> DEFAULT ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana'];
  DECLARE v_sql STRING;

  FOR coin_name IN (SELECT * FROM UNNEST(coins)) 
    DO
    
      -- Insert USD 24h Volume
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.volume_metrics.<table_name>_volume(
                      `date`, currency, total_24h_volume
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'USD' AS currency,
                      SUM(vol_24h) AS total_24h_volume
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'USD'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;

      -- Insert EURO 24h Volume
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.volume_metrics.<table_name>_volume(
                      `date`, currency, total_24h_volume
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'EUR' AS currency,
                      SUM(vol_24h) AS total_24h_volume
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'EUR'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;

      -- Insert INR 24h Volume
      SET v_sql = """
                  INSERT INTO crypto-data-analysis-441505.volume_metrics.<table_name>_volume(
                      `date`, currency, total_24h_volume
                  )
                  SELECT
                      DATE(last_updated) AS `date`,
                      'INR' AS currency,
                      SUM(vol_24h) AS total_24h_volume
                  FROM crypto-data-analysis-441505.final_data.tab_<table_name>
                  WHERE currency = 'INR'
                  AND DATE(last_updated) = CURRENT_DATE()
                  GROUP BY DATE(last_updated)
                  """;
      SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
      EXECUTE IMMEDIATE v_sql;

  END FOR;

  EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = ('Error in sp_24hr_volume_analysis ' || @@error.message);
END;