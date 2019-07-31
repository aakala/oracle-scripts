CREATE OR REPLACE PROCEDURE compress_part (i_table_owner      IN VARCHAR2,                            i_table_name       IN VARCHAR2,                            i_partition_name   IN VARCHAR2)   AS      v_temp                 NUMBER := 0;      v_part_name            VARCHAR2 (30);      v_table_name           VARCHAR2 (30);      v_table_owner          VARCHAR2 (30);      v_lock_sql             LONG;      v_alter_sql            LONG;      v_result               NUMBER := 0;      v_result_ind           VARCHAR2 (30);      v_tbs_name             VARCHAR2 (30);      v_ind_part_name        VARCHAR2 (30);      v_ind_subpart_name     VARCHAR2 (30);      v_ind_rebuild_sql      LONG;      v_ind_noparallel_sql   LONG;      v_stats_sql           LONG;
      FUNCTION check_valid_part (in_schema    VARCHAR2,                              in_table     VARCHAR2,                              in_part      VARCHAR2)      RETURN NUMBER   IS      v_ret   NUMBER := 0;   BEGIN      DBMS_OUTPUT.put_line (            'in_schema = '         || in_schema         || 'in_table ='         || in_table         || 'in_part ='         || in_part);
     SELECT COUNT (*)        INTO v_ret        from all_tab_partitions atp        where atp.table_owner = in_schema        and atp.table_name = in_table        and atp.partition_name = in_part;
      DBMS_OUTPUT.put_line ('the value evaluted for v_Ret is ' || v_ret);
      RETURN v_ret;   END check_valid_part;

 FUNCTION check_index_type (in_schema    VARCHAR2,                              in_table     VARCHAR2,                              in_part      VARCHAR2)      RETURN VARCHAR2   IS      v_ind_count      NUMBER := 0;      v_ind_part_cnt   NUMBER := 0;      v_index_type     VARCHAR2 (40);      v_ret_ind_type   VARCHAR2 (30);   BEGIN      /* The convention used is N indicates no indexes on the table, G indicates Global Index only,      P indicates partitioned indexes exist and S indicates Indexes with Sub-partitions exist*/
      --DBMS_OUTPUT.put_line ('in_schema = ' || in_schema || 'in_table =' ||in_table|| 'in_part =' || in_part);      SELECT COUNT (DISTINCT index_name)        INTO v_ind_count        FROM dba_indexes       WHERE table_owner = in_schema AND table_name = in_table;
      DBMS_OUTPUT.put_line ('v_ind_count = ' || v_ind_count);
      IF v_ind_count = 0      THEN         DBMS_OUTPUT.put_line (' The block where index type is set to N');         v_ret_ind_type := 'N';         RETURN v_ret_ind_type;      ELSE         /* As index exist we will check if they are partitioned or not */
         SELECT COUNT (*)           INTO v_ind_part_cnt           FROM dba_indexes          WHERE     table_owner = in_schema                AND table_name = in_table                AND partitioned = 'YES';
         IF v_ind_part_cnt = 0         THEN            /* As Index exsits on the table and it is not partitioned  thus it will be a global index */            DBMS_OUTPUT.put_line (' The block where index type is set to G');            v_ret_ind_type := 'G';            RETURN v_ret_ind_type;         ELSE            SELECT DISTINCT segment_type              INTO v_index_type              FROM dba_segments             WHERE owner = in_schema AND segment_name = in_table;
            IF v_index_type = 'TABLE PARTITION'            THEN               DBMS_OUTPUT.put_line (                  ' The block where index type is set to P');               v_ret_ind_type := 'P';               RETURN v_ret_ind_type;            ELSIF v_index_type = 'TABLE SUBPARTITION'            THEN               DBMS_OUTPUT.put_line (                  ' The block where index type is set to S');               v_ret_ind_type := 'S';               RETURN v_ret_ind_type;            END IF;         END IF;      END IF;   END check_index_type;

 BEGIN      v_table_owner := UPPER (i_table_owner);      v_table_name := UPPER (i_table_name);      v_part_name := UPPER (i_partition_name);
      /* Check for valid table name and partition name combination */
      --DBMS_OUTPUT.put_line ('Calling check_valid_part function');      v_result := check_valid_part (v_table_owner, v_table_name, v_part_name);
      --DBMS_OUTPUT.put_line ('the value retured v_result is ' || v_result);
      IF v_result = 0      THEN         DBMS_OUTPUT.put_line (            'This is not a valid Schema, Table, Partition Combination');         RETURN;      ELSE         /* As it is a valid combination do the actual work */         /* Lock the partition in exclusive mode and set the ddl_lock_timeout to wait incase any DML is in progress*/
         EXECUTE IMMEDIATE 'alter session set ddl_lock_timeout=1800';
         v_lock_sql :=               'lock table '            || v_table_owner            || '.'            || v_table_name            || ' partition ('            || v_part_name            || ' ) in exclusive mode';         DBMS_OUTPUT.put_line (v_lock_sql);         --execute immediate v_lock_sql ;
         /* check for  index type*/
         DBMS_OUTPUT.put_line ('Calling check_index_type function');         v_result_ind :=            check_index_type (v_table_owner, v_table_name, v_part_name);
         DBMS_OUTPUT.put_line (            'the value retured v_result_ind is ' || v_result_ind);
         /* Compress the partition with update for query high and "update global indexes" clause to take care of global indexes if any */
         v_alter_sql :=               'ALTER TABLE '            || v_table_owner            || '.'            || v_table_name            || ' MOVE PARTITION '            || v_part_name            || ' COMPRESS FOR QUERY HIGH UPDATE GLOBAL INDEXES ';
         DBMS_OUTPUT.put_line (v_alter_sql);
         EXECUTE IMMEDIATE v_alter_sql;
         /*  This blcok will Rebuild Local Indexes             The indexes are partitioned             Getting the List of  Partitioned indexes that need to be rebuilt        */         FOR n            IN (SELECT OWNER, index_name, degree                  FROM dba_indexes                 WHERE     table_owner = v_table_owner                       AND table_name = v_table_name                       AND partitioned = 'YES')         LOOP            BEGIN               /* Rebuild indexes depending whether they contain partitions or subpartitions */               IF v_result_ind = 'P'               THEN                  v_ind_part_name := NULL;                  v_tbs_name := NULL;
                  SELECT partition_name, tablespace_name                    INTO v_ind_part_name, v_tbs_name                    FROM dba_ind_partitions                   WHERE     index_owner = n.owner                         AND index_name = n.index_name                         AND partition_name = v_part_name;
                  v_ind_rebuild_sql :=                        ' alter index '                     || n.owner                     || '.'                     || n.index_name                     || ' rebuild partition '                     || v_ind_part_name                     || ' tablespace '                     || v_tbs_name                     || ' parallel 8 ';                  --v_ind_noparallel_sql := '';
                  DBMS_OUTPUT.put_line ( v_ind_rebuild_sql);
                  --dbms_output.put_line ('Partitions: v_ind_noparallel_sql is '||v_ind_noparallel_sql);
                  EXECUTE IMMEDIATE v_ind_rebuild_sql;               --execute immediate v_ind_noparallel_sql;
               ELSIF v_result_ind = 'S'               THEN                  /*v_ind_subpart_name := NULL;
                  SELECT partition_name, tablespace_name                    INTO v_ind_subpart_name, v_tbs_name                    FROM dba_ind_subpartitions                   WHERE index_owner = n.owner AND index_name = n.index_name;
                  v_ind_rebuild_sql :=                        'alter index '                     || n.owner                     || '.'                     || n.index_name                     || ' rebuild subpartition '                     || v_ind_subpart_name                     || ' tablespace '                     || v_tbs_name                     || ' parallel 12';                 -- v_ind_noparallel_sql := '';
                  DBMS_OUTPUT.put_line (                        'SubPartitions: v_ind_rebuild_sql is '                     || v_ind_rebuild_sql);                  DBMS_OUTPUT.put_line (                        'SubPartitions:  v_ind_noparallel_sql is '                     || v_ind_noparallel_sql);

               --execute immediate v_ind_rebuild_sql;               --execute immediate v_ind_noparallel_sql; */
               DBMS_OUTPUT.put_line ('TEST');
               END IF;            END;         END LOOP;
         --sys.dbms_stats.gather_table_stats(ownname=>''            || v_table_owner            || '',tabname=>''            || v_table_name            ||  '' ,cascade=>TRUE,degree=>8 , granularity=>'PARTITION', partname=> ''            || v_part_name            || '');

      END IF;
END compress_part;/
