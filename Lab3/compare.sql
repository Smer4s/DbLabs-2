CREATE OR REPLACE PROCEDURE GENERATE_SYNC_SCRIPT(
    p_dev_schema VARCHAR2,
    p_prod_schema VARCHAR2
) AUTHID CURRENT_USER IS

    -- Вспомогательная функция для сравнения исходного кода
    FUNCTION OBJECTS_DIFFERENT(
        p_object_name VARCHAR2,
        p_object_type VARCHAR2
    ) RETURN BOOLEAN IS
        v_dev_count NUMBER;
        v_prod_count NUMBER;
    BEGIN
        -- Сравнение количества строк
        SELECT COUNT(*) INTO v_dev_count
        FROM all_source
        WHERE owner = p_dev_schema
          AND name = p_object_name
          AND type = p_object_type;

        SELECT COUNT(*) INTO v_prod_count
        FROM all_source
        WHERE owner = p_prod_schema
          AND name = p_object_name
          AND type = p_object_type;

        IF v_dev_count != v_prod_count THEN
            RETURN TRUE;
        END IF;

        -- Построчное сравнение кода
        FOR r_dev IN (
            SELECT text FROM all_source
            WHERE owner = p_dev_schema
              AND name = p_object_name
              AND type = p_object_type
            ORDER BY line
        ) LOOP
            FOR r_prod IN (
                SELECT text FROM all_source
                WHERE owner = p_prod_schema
                  AND name = p_object_name
                  AND type = p_object_type
                ORDER BY line
            ) LOOP
                IF r_dev.text != r_prod.text THEN
                    RETURN TRUE;
                END IF;
            END LOOP;
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN TRUE;
    END;

BEGIN
    DBMS_OUTPUT.PUT_LINE('-- Schema synchronization script');
    DBMS_OUTPUT.PUT_LINE('-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('-- Source schema: ' || p_dev_schema);
    DBMS_OUTPUT.PUT_LINE('-- Target schema: ' || p_prod_schema);
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------');
    -- Indexes Section
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '/* INDEX DIFFERENCES */');
    -- New indexes
    FOR idx IN (
        SELECT 
            i.index_name, 
            i.table_name, 
            i.uniqueness, 
            LISTAGG(c.column_name, ', ') WITHIN GROUP (ORDER BY c.column_position) AS idx_columns -- Переименован алиас
        FROM all_indexes i
        JOIN all_ind_columns c 
            ON i.owner = c.index_owner 
            AND i.index_name = c.index_name
        WHERE i.owner = p_dev_schema
        AND i.index_name NOT LIKE '%_PK'
        AND i.index_name NOT IN (
            SELECT index_name 
            FROM all_indexes 
            WHERE owner = p_prod_schema
        )
        GROUP BY i.index_name, i.table_name, i.uniqueness
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('CREATE ' || idx.uniqueness || ' INDEX ' || p_prod_schema || '.' || idx.index_name
            || ' ON ' || p_prod_schema || '.' || idx.table_name || '(' || idx.idx_columns || ');'); -- Исправлен алиас
    END LOOP;

    -- Obsolete indexes
    FOR idx IN (
        SELECT index_name
        FROM all_indexes
        WHERE owner = p_prod_schema
          AND index_name NOT LIKE '%_PK'
          AND index_name NOT IN (
              SELECT index_name FROM all_indexes WHERE owner = p_dev_schema
          )
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || p_prod_schema || '.' || idx.index_name || ';');
    END LOOP;

    -- Procedures Section
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '/* PROCEDURE DIFFERENCES */');
    -- New procedures
    FOR obj IN (
        SELECT object_name
        FROM all_objects
        WHERE owner = p_dev_schema
          AND object_type = 'PROCEDURE'
          AND object_name NOT IN (
              SELECT object_name FROM all_objects 
              WHERE owner = p_prod_schema AND object_type = 'PROCEDURE'
          )
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('-- Create procedure: ' || obj.object_name);
        DBMS_OUTPUT.PUT_LINE('CREATE OR REPLACE ');
        FOR src IN (
            SELECT text
            FROM all_source
            WHERE owner = p_dev_schema
              AND name = obj.object_name
              AND type = 'PROCEDURE'
            ORDER BY line
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(src.text);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('/');
    END LOOP;

    -- Changed procedures
    FOR obj IN (
        SELECT object_name
        FROM all_objects
        WHERE owner = p_dev_schema
          AND object_type = 'PROCEDURE'
          AND object_name IN (
              SELECT object_name FROM all_objects 
              WHERE owner = p_prod_schema AND object_type = 'PROCEDURE'
          )
    ) LOOP
        IF OBJECTS_DIFFERENT(obj.object_name, 'PROCEDURE') THEN
            DBMS_OUTPUT.PUT_LINE('-- Replace procedure: ' || obj.object_name);
            DBMS_OUTPUT.PUT_LINE('DROP PROCEDURE ' || p_prod_schema || '.' || obj.object_name || ';');
            DBMS_OUTPUT.PUT_LINE('CREATE OR REPLACE ');
            FOR src IN (
                SELECT text
                FROM all_source
                WHERE owner = p_dev_schema
                  AND name = obj.object_name
                  AND type = 'PROCEDURE'
                ORDER BY line
            ) LOOP
                DBMS_OUTPUT.PUT_LINE(src.text);
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('/');
        END IF;
    END LOOP;

    -- Obsolete procedures
    FOR obj IN (
        SELECT object_name
        FROM all_objects
        WHERE owner = p_prod_schema
          AND object_type = 'PROCEDURE'
          AND object_name NOT IN (
              SELECT object_name FROM all_objects 
              WHERE owner = p_dev_schema AND object_type = 'PROCEDURE'
          )
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP PROCEDURE ' || p_prod_schema || '.' || obj.object_name || ';');
    END LOOP;

    -- Repeat similar logic for FUNCTIONS...

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '-- End of synchronization script');
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error generating script: ' || SQLERRM);
END;
/