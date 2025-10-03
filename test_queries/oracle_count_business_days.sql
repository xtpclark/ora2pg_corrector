CREATE OR REPLACE FUNCTION count_business_days (
    p_start_date IN DATE,
    p_end_date IN DATE
) RETURN NUMBER IS
    v_count NUMBER := 0;
    v_current_date DATE := TRUNC(p_start_date);
BEGIN
    WHILE v_current_date <= TRUNC(p_end_date) LOOP
        IF TO_CHAR(v_current_date, 'DY') NOT IN ('SAT', 'SUN') THEN
            v_count := v_count + 1;
        END IF;
        v_current_date := v_current_date + 1;
    END LOOP;
    
    RETURN v_count;
EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, 'Error in count_business_days: ' || SQLERRM);
END count_business_days;
/
