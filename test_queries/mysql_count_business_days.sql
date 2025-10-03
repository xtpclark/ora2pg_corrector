DELIMITER //
CREATE FUNCTION `count_business_days` (
    p_start_date DATE,
    p_end_date DATE
) RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_current_date DATE;
    
    SET v_current_date = p_start_date;
    
    WHILE v_current_date <= p_end_date DO
        IF DAYOFWEEK(v_current_date) NOT IN (1, 7) THEN
            SET v_count = v_count + 1;
        END IF;
        SET v_current_date = DATE_ADD(v_current_date, INTERVAL 1 DAY);
    END WHILE;
    
    RETURN v_count;
END //
DELIMITER ;
