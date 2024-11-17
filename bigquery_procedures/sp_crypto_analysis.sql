CREATE OR REPLACE PROCEDURE `crypto-data-analysis-441505.source_codes.sp_crypto_analysis`()
BEGIN

  DECLARE coins ARRAY<STRING> DEFAULT ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana'];
  DECLARE v_sql STRING;

  EXECUTE IMMEDIATE 'BEGIN TRANSACTION';

    FOR coin_name IN (SELECT * FROM UNNEST(coins)) 
      DO

        SET v_sql = """
                    INSERT INTO crypto-data-analysis-441505.final_data.tab_<table_name>
                        SELECT 
                          'USD' AS currency,
                          COALESCE(CAST(ROUND(usd, 2) AS FLOAT64), 0) AS value,
                          COALESCE(CAST(ROUND(usd_market_cap, 2) AS FLOAT64), 0) AS market_cap,
                          COALESCE(CAST(ROUND(usd_24h_vol, 2) AS FLOAT64), 0) AS vol_24h,
                          COALESCE(CAST(ROUND(usd_24h_change, 2) AS FLOAT64), 0) AS change_24h,
                          COALESCE(TIMESTAMP_SECONDS(CAST(usd_last_updated AS INT64)), TIMESTAMP(date)) AS last_updated
                        FROM crypto-data-analysis-441505.initial_data.crypto_data_temp
                        WHERE coin = CAST('<coin_name>' AS STRING)
                        AND `date` = CURRENT_DATE()

                        UNION ALL
                        
                        SELECT 
                          'INR' AS currency,
                          COALESCE(CAST(ROUND(inr, 2) AS FLOAT64), 0) AS value,
                          COALESCE(CAST(ROUND(inr_market_cap, 2) AS FLOAT64), 0) AS market_cap,
                          COALESCE(CAST(ROUND(inr_24h_vol, 2) AS FLOAT64), 0) AS vol_24h,
                          COALESCE(CAST(ROUND(inr_24h_change, 2) AS FLOAT64), 0) AS change_24h,
                          COALESCE(TIMESTAMP_SECONDS(CAST(inr_last_updated AS INT64)), TIMESTAMP(date)) AS last_updated
                        FROM crypto-data-analysis-441505.initial_data.crypto_data_temp
                        WHERE coin = CAST('<coin_name>' AS STRING)
                        AND `date` = CURRENT_DATE()

                        UNION ALL
                        
                        SELECT 
                          'EUR' AS currency,
                          COALESCE(CAST(ROUND(eur, 2) AS FLOAT64), 2) AS value,
                          COALESCE(CAST(ROUND(eur_market_cap, 2) AS FLOAT64), 2) AS market_cap,
                          COALESCE(CAST(ROUND(eur_24h_vol, 2) AS FLOAT64), 2) AS vol_24h,
                          COALESCE(CAST(ROUND(eur_24h_change, 2) AS FLOAT64), 2) AS change_24h,
                          COALESCE(TIMESTAMP_SECONDS(CAST(eur_last_updated AS INT64)), TIMESTAMP(date)) AS last_updated
                        FROM crypto-data-analysis-441505.initial_data.crypto_data_temp
                        WHERE coin = CAST('<coin_name>' AS STRING)
                        AND `date` = CURRENT_DATE()
                    """;
        SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
        SET v_sql = REPLACE(v_sql, '<coin_name>', CAST(coin_name.f0_ AS STRING));
        EXECUTE IMMEDIATE v_sql;

    END FOR;

    --Market cap analysis
    CALL `crypto-data-analysis-441505.source_codes.sp_market_cap_analysis`();
      
    --24h volume analysis
    CALL `crypto-data-analysis-441505.source_codes.sp_24hr_volume_analysis`();

  EXECUTE IMMEDIATE 'COMMIT TRANSACTION';

  EXCEPTION WHEN ERROR THEN
  RAISE USING MESSAGE = ('Error in sp_crypto_analysis '|| @@error.message);
END;