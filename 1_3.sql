CREATE TABLE DM.DM_F101_ROUND_F (
    FROM_DATE DATE,
    TO_DATE DATE,
    CHAPTER CHAR(1),
    LEDGER_ACCOUNT CHAR(5),
    CHARACTERISTIC CHAR(1),
    BALANCE_IN_RUB NUMERIC(23,8),
	R_BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
	R_BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
	R_BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
	R_TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
	R_TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
	R_TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
	R_TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
	R_TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
	R_TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
	R_BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
	R_BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8),
	R_BALANCE_OUT_TOTAL NUMERIC(23,8)
);

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
-- Объявление переменных
DECLARE 
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_log_id INT;
    v_error_message TEXT;
    v_from_date DATE;
    v_to_date DATE;
BEGIN
    -- Логирование начала выполнения
    SELECT NOW() INTO v_start_time;
    INSERT INTO logs.log_table (log_name, load_start_time, additional_info)
    VALUES ('fill_f101_round_f', v_start_time, 'Начало расчета 101 формы для даты: ' || i_OnDate)
    RETURNING log_id INTO v_log_id;

    -- Определяем границы отчетного периода
    v_from_date := i_OnDate - INTERVAL '1 month';
    v_to_date := i_OnDate - INTERVAL '1 day';

    BEGIN
        -- Удаляем старые записи за отчетный период
        DELETE FROM DM.DM_F101_ROUND_F 
		WHERE from_date = v_from_date 
		AND to_date = v_to_date;

        -- Заполнение витрины DM_F101_ROUND_F
        INSERT INTO DM.DM_F101_ROUND_F (
            FROM_DATE,
            TO_DATE,
            CHAPTER,
            LEDGER_ACCOUNT,
            CHARACTERISTIC,
            BALANCE_IN_RUB,
            R_BALANCE_IN_RUB,
            BALANCE_IN_VAL,
            R_BALANCE_IN_VAL,
            BALANCE_IN_TOTAL,
            R_BALANCE_IN_TOTAL,
            TURN_DEB_RUB,
            R_TURN_DEB_RUB,
            TURN_DEB_VAL,
            R_TURN_DEB_VAL,
            TURN_DEB_TOTAL,
            R_TURN_DEB_TOTAL,
            TURN_CRE_RUB,
            R_TURN_CRE_RUB,
            TURN_CRE_VAL,
            R_TURN_CRE_VAL,
            TURN_CRE_TOTAL,
            R_TURN_CRE_TOTAL,
            BALANCE_OUT_RUB,
            R_BALANCE_OUT_RUB,
            BALANCE_OUT_VAL,
            R_BALANCE_OUT_VAL,
            BALANCE_OUT_TOTAL,
            R_BALANCE_OUT_TOTAL
        )
        SELECT
            v_from_date,
            v_to_date,
            la."CHAPTER",
			-- Извлекаем первые 5 символов номера счета
            SUBSTRING(a."ACCOUNT_NUMBER", 1, 5), 
            a."CHAR_TYPE",
            -- Считаем сумму остатков и оборотов
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN bal_in.balance_out_rub 
				ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN bal_in.balance_out_rub 
				ELSE 0 END) / 1000 AS r_balance_in_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN bal_in.balance_out_rub 
				ELSE 0 END) AS balance_in_val,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN bal_in.balance_out_rub 
				ELSE 0 END) / 1000 AS r_balance_in_val,
            SUM(bal_in.balance_out_rub) AS balance_in_total,
            SUM(bal_in.balance_out_rub) / 1000 AS r_balance_in_total,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN turn.debet_amount_rub 
				ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN turn.debet_amount_rub 
				ELSE 0 END) / 1000 AS r_turn_deb_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN turn.debet_amount_rub 
				ELSE 0 END) AS turn_deb_val,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN turn.debet_amount_rub 
				ELSE 0 END) / 1000 AS r_turn_deb_val,
            SUM(turn.debet_amount_rub) AS turn_deb_total,
            SUM(turn.debet_amount_rub) / 1000 AS r_turn_deb_total,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN turn.credit_amount_rub 
				ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN turn.credit_amount_rub 
				ELSE 0 END) / 1000 AS r_turn_cre_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN turn.credit_amount_rub 
				ELSE 0 END) AS turn_cre_val,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN turn.credit_amount_rub 
				ELSE 0 END) / 1000 AS r_turn_cre_val,
            SUM(turn.credit_amount_rub) AS turn_cre_total,
            SUM(turn.credit_amount_rub) / 1000 AS r_turn_cre_total,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN bal_out.balance_out_rub 
				ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" IN ('810', '643') 
			    THEN bal_out.balance_out_rub 
				ELSE 0 END) / 1000 AS r_balance_out_rub,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN bal_out.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(CASE WHEN a."CURRENCY_CODE" NOT IN ('810', '643') 
			    THEN bal_out.balance_out_rub 
				ELSE 0 END) / 1000 AS r_balance_out_val,
            SUM(bal_out.balance_out_rub) AS balance_out_total,
            SUM(bal_out.balance_out_rub) / 1000 AS r_balance_out_total
        FROM DS.MD_ACCOUNT_D a
        JOIN ds.md_ledger_account_s la 
		    ON SUBSTRING(a."ACCOUNT_NUMBER", 1, 5) = la."LEDGER_ACCOUNT"::TEXT
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F bal_in 
		    ON a."ACCOUNT_RK" = bal_in.account_rk 
		    AND bal_in.on_date = v_from_date - INTERVAL '1 day'
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F bal_out 
		    ON a."ACCOUNT_RK" = bal_out.account_rk AND bal_out.on_date = v_to_date
        LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F turn 
		    ON a."ACCOUNT_RK" = turn.account_rk 
		    AND turn.on_date BETWEEN v_from_date AND v_to_date
            WHERE v_to_date BETWEEN a."DATA_ACTUAL_DATE" AND a."DATA_ACTUAL_END_DATE"
        GROUP BY v_from_date, v_to_date, la."CHAPTER", 
		SUBSTRING(a."ACCOUNT_NUMBER", 1, 5), a."CHAR_TYPE";

        -- Логирование окончания
        SELECT NOW() INTO v_end_time;
        UPDATE logs.log_table SET load_end_time = v_end_time WHERE log_id = v_log_id;

		-- Обработка ошибок
    EXCEPTION WHEN OTHERS THEN
        SELECT NOW() INTO v_end_time;
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        UPDATE logs.log_table SET load_end_time = v_end_time
		, error_message = v_error_message WHERE log_id = v_log_id;
        RAISE;
    END;
END;
$$;


-- Расчет 101 формы за январь 2018 года
CALL dm.fill_f101_round_f('2018-02-01'::DATE);

SELECT * FROM logs.log_table;