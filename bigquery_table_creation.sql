CREATE OR REPLACE PROCEDURE `crypto-data-analysis-441505.source_codes.sp_create_table_structures`()
BEGIN

    DECLARE coins ARRAY<STRING> DEFAULT ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana'];
    DECLARE v_sql STRING;

    FOR coin_name IN (SELECT * FROM UNNEST(coins)) 
        DO

            SET v_sql = """
                        CREATE TABLE IF NOT EXISTS crypto-data-analysis-441505.final_data.tab_<table_name>(
                            currency STRING,
                            `value` FLOAT64,
                            market_cap FLOAT64,
                            vol_24h FLOAT64,
                            change_24h FLOAT64,
                            last_updated TIMESTAMP
                        );
                        """;
            SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
            SET v_sql = REPLACE(v_sql, '<coin_name>', CAST(coin_name.f0_ AS STRING));
            EXECUTE IMMEDIATE v_sql;

            --Market cap Tables
            SET v_sql = """
                        CREATE TABLE IF NOT EXISTS crypto-data-analysis-441505.market_metrics.<table_name>_market_cap(
                            `date` DATE NOT NULL,
                            currency STRING NOT NULL,
                            total_market_cap FLOAT64 NOT NULL
                        );
                        """;
            SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
            SET v_sql = REPLACE(v_sql, '<coin_name>', CAST(coin_name.f0_ AS STRING));
            EXECUTE IMMEDIATE v_sql;

            --24h volume Tables
            SET v_sql = """
                        CREATE TABLE IF NOT EXISTS crypto-data-analysis-441505.volume_metrics.<table_name>_volume(
                            `date` DATE NOT NULL,
                            currency STRING NOT NULL,
                            total_24h_volume FLOAT64 NOT NULL
                        );
                        """;
            SET v_sql = REPLACE(v_sql, '<table_name>', CAST(coin_name.f0_ AS STRING));
            SET v_sql = REPLACE(v_sql, '<coin_name>', CAST(coin_name.f0_ AS STRING));
            EXECUTE IMMEDIATE v_sql;

    END FOR;

    EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = ('Error in sp_create_table_structures '||@@error.message);
END;