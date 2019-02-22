OPTIONS THREADS SASTRACE = ',,,sa' SASTRACELOC = SASLOG NOSTSUFFIX;

/* turn off display of outputs/notes */
%MACRO ods_off;
    ODS EXCLUDE ALL;
    ODS NORESULTS;
    OPTIONS NONOTES;
%MEND;
 
/* re-enable display of outputs/notes */
%MACRO ods_on;
    ODS EXCLUDE NONE;
    ODS RESULTS;
    OPTIONS NOTES;
%MEND;

%ods_off


/***********************************
Section 0: Set macro parameters
***********************************/

/* Set intermediate/final output data location */
LIBNAME myfiles "/sasprod/users/wleone/avar_recon";
/* Client-TPA Availability datasets */
LIBNAME sasdata "/sasprod/ca/sasdata/Client_TPA_Availability";
/* Set up SAS-SQL connection config data */
%INCLUDE "/prg/sasutil/sqlcode/sasuser_sql_creds.sas" ;
%LET evh_dw_r_conn = %CMPRES(
    DSN%BQUOTE(=)"IPE1PR_DWDB_003_PRD_EVH_DW_R"
    USER%BQUOTE(=)"&sasusr"
    PASSWORD%BQUOTE(=)"&saspwd"
    READBUFF%BQUOTE(=)1000
    );
%LET prd_r_conn = %CMPRES(
    DSN%BQUOTE(=)"IPE1PR_PRDANALYTICS_R"
    USER%BQUOTE(=)"&sasusr"
    PASSWORD%BQUOTE(=)"&saspwd"
    READBUFF%BQUOTE(=)1000
    );
%LET prd_w_conn = %CMPRES(
    DSN%BQUOTE(=)"IPE1PR_PRDANALYTICS_W"
    USER%BQUOTE(=)"&sasusr"
    PASSWORD%BQUOTE(=)"&saspwd"
    READBUFF%BQUOTE(=)1000
    );
LIBNAME prd_w SQLSVR &prd_w_conn.;

/* input parameters */
%LET client_list = ('client_x');
%LET start_yearmo = 201701;
%LET target_dataset = Eligibility;
%LET var_list_init = %SYSFUNC(LOWCASE(memberID~monthid~eligibility_type));
    /* Variables to include when available */
%LET avar_ind = 'Yes'; /* Include all AVARs when available? */

/***********************************
Section 1: Prepare the query inputs.
***********************************/

%MACRO add_avars;
    /* Create a lookup table for generating
       formatted SQL snippets for AVAR
       queries */
    %IF &avar_ind. = 'Yes'
    %THEN
        %DO;
           
            PROC SQL NOPRINT;

                CREATE TABLE avar_xwalk (
                    avar_num VARCHAR(2)
                    , pdw_avars VARCHAR(6)
                    , edw_avars VARCHAR(7)
                    , clm_avars VARCHAR(11)
                    , elig_avars VARCHAR(12)
                    , pdw_avar_checks VARCHAR(500)
                    , pdw_avar_check_sums VARCHAR(500)
                    , str_check_sums VARCHAR(500)
                    , pdw_edw_avar_comps VARCHAR(1500)
                    , clm_elig_avar_comps VARCHAR(1500)
                    )
                ;

            QUIT;
            
            %DO index=1 %TO 25;
                PROC SQL NOPRINT;

                    INSERT INTO avar_xwalk
                    VALUES (
                        "&index."
                        , "avar&index."
                        , "avar_&index."
                        , "clm%BQUOTE(.)avar_&index."
                        , "elig%BQUOTE(.)avar_&index."
                        , "%CMPRES(
                                CASE
                                    WHEN (avar&index. IS NULL
                                          OR UPCASE(CATS(avar&index.)) = 'N/A')
                                    THEN 0
                                    ELSE 1
                                END AS check_avar&index.
                          )"
                        , "%CMPRES(
                                CASE
                                    WHEN SUM(check_avar&index.) = 0
                                    THEN ''
                                    ELSE 'avar&index.'
                                END AS check_sum_avar&index.
                          )"
                        , "check_sum_avar&index."
                        , "%CMPRES(
                            COUNT(CASE
                                    WHEN RTRIM(LTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX))))
                                            = RTRIM(LTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX))))
                                        OR ((elig.avar_&index. IS NULL
                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                            AND (pdw.avar&index. IS NULL
                                             OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = 'N/A'
                                             OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = ''))
                                    THEN pdw.memberid
                                END) AS pdw_edw_avar&index.
                              %BQUOTE(,) ROUND(1.0 * (
                                    CASE
                                        WHEN COUNT(pdw.memberid) =
                                            COUNT(
                                                CASE
                                                WHEN RTRIM(LTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX))))
                                                        = RTRIM(LTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX))))
                                                    OR ((elig.avar_&index. IS NULL
                                                         OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                         OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                                        AND (pdw.avar&index. IS NULL
                                                         OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = 'N/A'
                                                         OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = ''))
                                                THEN pdw.memberid
                                                END)
                                        THEN 1 
                                        WHEN COUNT(pdw.memberid) > 0
                                        THEN 1.0/COUNT(pdw.memberid)
                                            * COUNT(
                                                CASE
                                                WHEN RTRIM(LTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX))))
                                                        = RTRIM(LTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX))))
                                                    OR ((elig.avar_&index. IS NULL
                                                         OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                         OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                                        AND (pdw.avar&index. IS NULL
                                                         OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = 'N/A'
                                                         OR LTRIM(RTRIM(CAST(pdw.avar&index. AS VARCHAR(MAX)))) = ''))
                                                THEN pdw.memberid
                                                END)
                                        ELSE 0
                                    END), 4) AS pdw_edw_avar&index._percent
                            )"
                        , "%CMPRES(
                            COUNT(
                                CASE
                                    WHEN elig.avar_&index. = clm.avar_&index.
                                        OR ((elig.avar_&index. IS NULL
                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                            AND (clm.avar_&index. IS NULL
                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = ''))
                                    THEN mmf.MEMBER_DIM_KEY
                                END) AS clm_elig_avar&index.
                            %BQUOTE(,) ROUND(1.0 * (
                                    CASE
                                        WHEN COUNT(mmf.MEMBER_DIM_KEY) =
                                            COUNT(
                                                CASE
                                                    WHEN elig.avar_&index. = clm.avar_&index.
                                                        OR ((elig.avar_&index. IS NULL
                                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                                            AND (clm.avar_&index. IS NULL
                                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = ''))
                                                    THEN mmf.MEMBER_DIM_KEY
                                                END)
                                        THEN 1 
                                        WHEN COUNT(mmf.MEMBER_DIM_KEY) > 0
                                        THEN 1.0/COUNT(mmf.MEMBER_DIM_KEY)
                                            * COUNT(
                                                CASE
                                                    WHEN elig.avar_&index. = clm.avar_&index.
                                                        OR ((elig.avar_&index. IS NULL
                                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                             OR LTRIM(RTRIM(CAST(elig.avar_&index. AS VARCHAR(MAX)))) = '')
                                                            AND (clm.avar_&index. IS NULL
                                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = 'N/A'
                                                             OR LTRIM(RTRIM(CAST(clm.avar_&index. AS VARCHAR(MAX)))) = ''))
                                                    THEN mmf.MEMBER_DIM_KEY
                                                END)
                                        ELSE 0
                                    END), 4) AS clm_elig_avar&index._percent
                            )"
                        , )
                        

                    ;

                QUIT;

            %END;

            %GLOBAL
                pdw_avars
                edw_avars
                ;

            PROC SQL NOPRINT;
                SELECT pdw_avars
                INTO :pdw_avars SEPARATED BY ', '
                FROM avar_xwalk
                ;
                SELECT edw_avars
                INTO :edw_avars SEPARATED BY ', '
                FROM avar_xwalk
                ;
            QUIT;

            %LET var_list_init =
                %SYSFUNC(CATS(&var_list_init., ~, 
                    %SYSFUNC(TRANWRD(%BQUOTE(&pdw_avars.), %BQUOTE(, ), ~))));
            
        %END;

    %ELSE %RETURN;

%MEND add_avars;
%add_avars

%LET all_var_no = %SYSFUNC(COUNTW(&var_list_init., '~ '));
%LET quoted_var_list = ;

%MACRO create_quoted_item_list;
    %DO a=1 %TO &all_var_no.;
        %IF &a. = 1
        %THEN %DO;
            %LET quoted_var_list = %QSCAN(%BQUOTE(&var_list_init.), &a., %NRQUOTE(~ ));
            %LET quoted_var_list = "%SYSFUNC(COMPRESS(&quoted_var_list., ,nk))";
        %END;
        %ELSE %DO;
            %LET iter_var = %QSCAN(%BQUOTE(&var_list_init.), &a., %NRQUOTE(~ ));
            %LET quoted_var_list = %SYSFUNC(CATX(%NRQUOTE(, )
                              , &quoted_var_list.
                              , "&iter_var."));
        %END;
    %END;
  
    %LET quoted_var_list = &quoted_var_list.;
%MEND;
%create_quoted_item_list

/* Pull client-TPA data based on input client names */
DATA myfiles.all_pdw_and_edw
        (RENAME=(
            PDW_Client_Name=Client_Name
            PDW_TPA_Name=TPA_Name));
    SET sasdata.all_pdw_and_edw;
    WHERE PDW_Client_Name IN &client_list.;
    ref_id = _N_;
RUN;

/* Store # of PDW client-TPA's
   as macro variable */
PROC SQL NOPRINT;
    SELECT CATS(COUNT(*))
    INTO :count
    FROM myfiles.all_pdw_and_edw
    ;
QUIT;

/* Set variables to be used when
   auto-creating libnames */
PROC SQL NOPRINT;
    SELECT
        client_name
        , tpa_name
        , CATS('lref', ref_id)
        , path
        , client_id
        , tpa_id
    INTO
        :client_1-:client_&count.
        , :tpa_1-:tpa_&count.
        , :libref_1-:libref_&count.
        , :path_1-:path_&count.
        , :cid_1-:cid_&count.
        , :tid_1-:tid_&count.
    FROM myfiles.all_pdw_and_edw
    ;
QUIT;


%MACRO comp_loop(startdt);

    /* Begin looping through the client-TPAs
       within the chosen year-month */
    %DO k = 1 %TO &count. ;

        /* Delete all variables created during
           last client-TPA's loop (if code run
           was interrupted) */
        %SYMDEL
            query_vars
            query_vars_list
            query_vars_list_nocommas
            pdw_avar_checks
            pdw_avar_check_sums
            str_check_sums
            elig_type_check
            pdw_edw_avar_comps
            clm_elig_avar_comps
            check_sum
            query_vars_list_nocommas2
            total_pdw_mm
            distinct_pdw_mm
            total_elig_mm
            distinct_elig_mm
            /NOWARN;

        %PUT Deleting the PDW table from EDW if it exists. Otherwise, there will be a non-terminating error message.;

        PROC SQL NOPRINT NOERRORSTOP;

            CONNECT TO SQLSVR (&prd_w_conn.) ;
                EXECUTE(
                        DROP TABLE wdl_pdw_avar;
                        DROP TABLE wdl_edw_elig;
                        )
                    BY SQLSVR
                ;

            DISCONNECT FROM SQLSVR;

        QUIT;

        /* Skip the following client-TPA's
        %IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "slhp-pacificsource"))
        %THEN %GOTO enditer;
        %ELSE;
        
        %IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "c3-masshealth")) 
        %THEN %GOTO enditer;
        %ELSE;
        

        %IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "slhp-idaho"))
        %THEN %GOTO enditer;
        %ELSE;

        %IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "slhp-cmms"))
        %THEN %GOTO enditer;
        %ELSE;
        */

        %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) %SYSFUNC(CATS(Loop &k./%CMPRES(&count.) (&&client_&k.. - &&tpa_&k..).));

        /***********************************
        Section 2: Create a SAS dataset of
                   the needed PDW data.
        ***********************************/

        %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) Pushing %BQUOTE(PDWs) Eligibility data into EDW%BQUOTE(.);

        LIBNAME &&libref_&k.. "&&Path_&k.."
            ACCESS = Read;

        /* Client_X Eligbility dataset unavailable 1/16 - 1/19 */
        %IF %SYSFUNC(CATS(&&cid_&k.)) = %SYSFUNC(CATS(100))
        %THEN
            %DO;
                LIBNAME &&libref_&k.. "/sasprod/client_x/sasdata/last"
                ACCESS = Read;
            %END;
        %ELSE;
        

        
        /* If so, list variables in the target dataset */
        PROC CONTENTS DATA = &&libref_&k...&target_dataset.
            MEMTYPE = DATA    
            OUT = var_check
            NOPRINT;
        RUN;

        /* Only continue if some target
           variables exists in this dataset. */
        PROC SQL NOPRINT;
            SELECT
                COUNT(*)
            INTO :query_vars
            FROM var_check
            WHERE name IN (&quoted_var_list.)
            ;
        QUIT;

        PROC SQL NOPRINT;
            /* count dataset records */
            SELECT
                COUNT(*)
            INTO :total_records
            FROM &&libref_&k...&target_dataset.
            ;
            
            /* remove variables from input list
               based on whether they are in the 
               current dataset */
            SELECT
                name
            INTO :query_vars_list SEPARATED BY ", "
            FROM var_check
            WHERE CATS(name) IN (&quoted_var_list.)
            ;

            SELECT
                name
            INTO :query_vars_list_nocommas SEPARATED BY " "
            FROM var_check
            WHERE CATS(name) IN (&quoted_var_list.)
            ;

            SELECT
                pdw_avar_checks
                , pdw_avar_check_sums
                , str_check_sums
            INTO
                :pdw_avar_checks SEPARATED BY ', '
                , :pdw_avar_check_sums SEPARATED BY ', '
                , :str_check_sums SEPARATED BY ' '
            FROM avar_xwalk
            WHERE FIND("&query_vars_list_nocommas.", CATS(pdw_avars)) > 0
            ;

            /* Ensure eligibility_type is populated if it exists */
            %IF %SYSFUNC(FIND(&query_vars_list_nocommas., eligibility_type))
            %THEN
                %DO;
                    SELECT COUNT(*)
                    INTO :elig_type_check
                    FROM &&libref_&k...&target_dataset.
                    WHERE
                        (%IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "c3-masshealth"))
                        %THEN %STR(_eligmed = 1);
                        %ELSE %STR(eligmed = 1);)
                        AND monthid = &startdt.
                        %IF %SYSFUNC(FIND(&query_vars_list_nocommas., eligibility_type))
                        /* FE - Full Eligibility; CA - Confirmed Attribution. */
                        /* FIND evaluates to 0 unless the substring
                           exists in the target string. %IF executes
                           %THEN only when the %IF expression
                           evaluates to a non-zero integer. */
                        %THEN %STR(AND (eligibility_type = "CA" 
                                OR eligibility_type = "FE"));
                        %ELSE;
                    ;
                %END;
            %ELSE;

        QUIT;

        /* Create a table with the PDW
           Eligibility data in EDW's
           PRD003 PRDAnalytics */

        PROC SQL NOPRINT /*_method _tree*/;
            CONNECT TO SQLSVR (&evh_dw_r_conn.) ;
                CREATE TABLE prd_w.wdl_pdw_avar AS (
                    SELECT
                        %STR(&query_vars_list.)
                        , %STR(&pdw_avar_checks.)
                    FROM &&libref_&k...&target_dataset.
                    WHERE  /* strict equalities used for faster equijoin processing: https://www.sas.com/content/dam/SAS/en_gb/doc/presentations/user-groups/proc-sql-tuning-101-30.pdf */
                        (%IF %SYSFUNC(FIND("&&client_&k..-&&tpa_&k..", "c3-masshealth"))
                         %THEN %STR(_eligmed = 1);
                         %ELSE %STR(eligmed = 1);)
                        AND monthid = &startdt.
                        %IF %SYSFUNC(FIND(&query_vars_list_nocommas., eligibility_type))
                            /* FE - Full Eligibility; CA - Confirmed Attribution. */
                            /* FIND evaluates to 0 unless the substring
                               exists in the target string. %IF executes
                               %THEN only when the %IF expression
                               evaluates to a non-zero integer. */
                        %THEN
                            %DO;
                                %IF &elig_type_check.
                                %THEN %STR(AND (eligibility_type = "CA" 
                                    OR eligibility_type = "FE")); 
                                %ELSE;
                            %END;
                        %ELSE;
                    );
            
            DISCONNECT FROM SQLSVR;

        QUIT;

        PROC SQL NOPRINT;
            
            /* Check which of the AVARs are truly
               populated with non-trivial values */
            CREATE TABLE _check AS
                SELECT
                    &pdw_avar_check_sums.
                FROM prd_w.wdl_pdw_avar
                ;

        QUIT;
        
        /* Create a list of those AVARs that are both
           (a) available and (b) populated with
           non-trivial values in PDW. */
        PROC TRANSPOSE DATA=_check
            OUT=check;
            VAR &str_check_sums.;
        RUN;

        PROC SQL NOPRINT;

            SELECT COL1
            INTO :check_sum SEPARATED BY ' '
            FROM check
            ;

            /* Update the query_vars_list accordingly */
            SELECT
                name
            INTO :query_vars_list_nocommas2 SEPARATED BY " "
            FROM var_check
            WHERE (FIND("&query_vars_list_nocommas.", CATS(name)) > 0
                    AND FIND(name, 'avar') = 0)
                OR FIND("&check_sum.", CATS(name)) > 0
            ;

        QUIT;

        /***********************************
        Section 3: For each of the 25 EDW
                   AVAR fields that are also
                   in the PDW dataset, pull
                   each record's AVAR value
                   and create a PDW-EDW
                   comparison dataset.

        The purpose of this looping process
        is to ensure the EDW pulls don't
        exceed a reasonable size and crash
        the server.
        ***********************************/

        %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) Pulling EDW eligibility and claims data%BQUOTE(.);
        %LET mmf_id = mmf%BQUOTE(.)client_ID %BQUOTE(=) &&cid_&k.. AND mmf%BQUOTE(.)TPA_ID %BQUOTE(=) &&tid_&k..;
        %LET mcf_id = mcf%BQUOTE(.)client_ID %BQUOTE(=) &&cid_&k.. AND mcf%BQUOTE(.)TPA_ID %BQUOTE(=) &&tid_&k..;
           
        PROC SQL NOPRINT;
            /* Check which AVARs to include in EDW queries */
            SELECT
                pdw_edw_avar_comps
                , clm_elig_avar_comps
            INTO
                :pdw_edw_avar_comps SEPARATED BY ', '
                , :clm_elig_avar_comps SEPARATED BY ', '
            FROM avar_xwalk
            WHERE FIND("&query_vars_list_nocommas2.", CATS(pdw_avars)) > 0
            ;

            SELECT pdw_avars
            INTO :variable_chk SEPARATED BY ', '
            FROM avar_xwalk
            WHERE FIND("&query_vars_list_nocommas2.", CATS(pdw_avars)) > 0
            ;

            %PUT AVARs to be compared: &variable_chk..;

        QUIT;

        PROC SQL NOPRINT /*_method _tree*/;
            CONNECT TO SQLSVR (&evh_dw_r_conn.) ;
                CREATE TABLE prd_w.wdl_edw_elig AS (
                    SELECT * FROM CONNECTION TO SQLSVR (
                        SELECT    /* distinct as query pulls at member-month level */
                            member_dim_key
                            , avar_dim_key
                            , plan_dim_key
                        FROM EVH_DW.DBO.MEMBER_MONTH_FACT (NOLOCK)
                        WHERE confirmed_attribution_cnt = 1
                            AND Client_ID = &&cid_&k.. 
                            AND TPA_ID = &&tid_&k..
                            AND month_key IN (
                                SELECT date_dim_key
                                FROM EVH_DW.DBO.DATE_DIM
                                WHERE year_month = &itrdt.
                                )
                            AND plan_dim_key IN (
                                SELECT plan_dim_key
                                FROM EVH_DW.DBO.PLAN_DIM (NOLOCK)
                                WHERE Client_ID = &&cid_&k.. 
                                    AND TPA_ID = &&tid_&k..
                                    AND medical_flag = 'Y'
                                )
                        ;
                    ));
            
            DISCONNECT FROM SQLSVR;

        QUIT;
        
        PROC SQL NOPRINT /*_method _tree*/;
            CONNECT TO SQLSVR (&evh_dw_r_conn.) ;
                CREATE TABLE myfiles.edw_elig_lref&k._&startdt. AS (
                    SELECT * FROM CONNECTION TO SQLSVR (
                        SELECT 
                            COUNT(*) AS total_elig_mm
                            /* count of EDW eligibility records */
                            , COUNT(pdw.memberid)
                              /* count of PDW eligibility records */
                              AS matching_records
                            , ROUND(1.0 * COUNT(pdw.memberid)/COUNT(*), 4)
                              AS matching_percent
                            %IF %SYMEXIST(pdw_edw_avar_comps)
                            %THEN %BQUOTE(,) &pdw_edw_avar_comps.;
                                /* for each AVAR, count the number
                                   of PDW eligibility records with
                                   a corresponding EDW eligibility
                                   record where the AVAR values
                                   also match. */
                            %ELSE %PUT %CMPRES(No AVAR comps -
                                check that there are non-trivial
                                PDW AVAR values for this year-month%BQUOTE(.));
                        FROM PRDAnalytics.DBO.wdl_edw_elig as mmf
                        LEFT JOIN (   /* 1:1 map with mmf on member_dim_key */
                                SELECT /* distinct as query pulls table's index */
                                    member_dim_key
                                    , member_nbr
                                FROM EVH_DW.DBO.MEMBER_DIM (NOLOCK)
                                WHERE Client_ID = &&cid_&k.. 
                                    AND TPA_ID = &&tid_&k..    
                                ) AS md
                            ON mmf.member_dim_key = md.member_dim_key
                        LEFT JOIN EVH_DW.DBO.AVAR_DIM AS elig (NOLOCK)
                            ON mmf.avar_dim_key = elig.avar_dim_key
                        LEFT JOIN PRDAnalytics.DBO.wdl_pdw_avar AS pdw (NOLOCK)
                            ON pdw.memberid = md.member_nbr
                        /* PDW data pre-filtered by client, TPA, and month */
                        ;
                    ));
            
            DISCONNECT FROM SQLSVR;

        QUIT;

        PROC SQL NOPRINT /*_method _tree*/;
            CONNECT TO SQLSVR (&evh_dw_r_conn.) ;
                CREATE TABLE myfiles.edw_claims_lref&k._&startdt. AS (
                    SELECT * FROM CONNECTION TO SQLSVR (
                        SELECT
                            COUNT(*) AS total_clm_mm
                            /* count of both eligible and ineligible claims */
                            , COUNT(mmf.MEMBER_DIM_KEY)
                              /* count restricted to eligible claims */
                              AS matching_records
                            , ROUND(1.0 * COUNT(mmf.MEMBER_DIM_KEY)/COUNT(*), 4)
                              AS matching_percent
                            %IF %SYMEXIST(clm_elig_avar_comps)
                            %THEN %BQUOTE(,) &clm_elig_avar_comps.;
                                /* for each AVAR, count the number
                                   of eligible claims where AVAR_DIM
                                   matches the eligibility record's
                                   AVAR_DIM value; then divide by
                                   by total eligible claims */
                            %ELSE;
                        FROM (    /* No filter for eligibility's confirmed_attribution_cnt */
                            SELECT    /* purposely not distinct: need to
                                      count every MCF claim */
                                    member_dim_key
                                    , avar_dim_key
                                    , plan_dim_key
                                FROM EVH_DW.DBO.MEDICAL_CLAIM_FACT (NOLOCK)
                                WHERE Client_ID = &&cid_&k.. 
                                    AND TPA_ID = &&tid_&k..
                                    AND service_date_key IN (
                                        SELECT date_dim_key
                                        FROM EVH_DW.DBO.DATE_DIM
                                        WHERE year_month = &itrdt.
                                        )
                                    AND plan_dim_key IN (
                                        SELECT plan_dim_key
                                        FROM PRDAnalytics.DBO.wdl_edw_elig
                                        )
                                ) as mcf
                        LEFT JOIN PRDAnalytics.DBO.wdl_edw_elig as mmf
                            /* 0-to-many claims per member-month */
                            ON mcf.plan_dim_key = mmf.plan_dim_key
                                AND mcf.MEMBER_DIM_KEY = mmf.MEMBER_DIM_KEY
                        LEFT JOIN EVH_DW.DBO.AVAR_DIM AS clm (NOLOCK)
                            ON mcf.avar_dim_key = clm.avar_dim_key
                        LEFT JOIN EVH_DW.DBO.AVAR_DIM AS elig (NOLOCK)
                            ON mmf.avar_dim_key = elig.avar_dim_key
                        ;
            ));

            DISCONNECT FROM SQLSVR;

        QUIT;

        %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) Consolidating AVAR comps for &startdt.%BQUOTE(.);

        PROC SQL NOPRINT;

            SELECT COUNT(*)
            INTO :total_pdw_mm
            FROM prd_w.wdl_pdw_avar
            ;

            SELECT total_elig_mm
            INTO :total_elig_mm
            FROM myfiles.edw_elig_lref&k._&startdt.
            ;

            CREATE TABLE myfiles.pdw_edw_comp_lref&k._&startdt. AS
                SELECT
                    "&&client_&k.." AS Client_Name
                    , "&&tpa_&k.." AS TPA_Name
                    , "&&cid_&k.." AS Client_ID
                    , "&&tid_&k.." AS TPA_ID
                    , "&&path_&k.." AS Path
                    , "&target_dataset." AS Core_Dataset
                    , "&startdt." AS Monthid
                    , INPUT("&total_pdw_mm.", 12.)
                      AS total_pdw_mm
                    , elig.*
                FROM myfiles.edw_elig_lref&k._&startdt. AS elig
                ;

            CREATE TABLE myfiles.clm_elig_comp_lref&k._&startdt. AS
                SELECT
                    "&&client_&k.." AS Client_Name
                    , "&&tpa_&k.." AS TPA_Name
                    , "&&cid_&k.." AS Client_ID
                    , "&&tid_&k.." AS TPA_ID
                    , "&&path_&k.." AS Path
                    , "&target_dataset." AS Core_Dataset
                    , "&startdt." AS Monthid
                    , INPUT("&total_elig_mm.", 12.)
                      AS total_elig_mm
                    , clm.*
                FROM myfiles.edw_claims_lref&k._&startdt. AS clm
                ;            

        QUIT;
        
        /* Delete the PDW table from EDW if
           it exists */
        PROC SQL NOPRINT NOERRORSTOP;

            CONNECT TO SQLSVR (&prd_w_conn.) ;
                EXECUTE(
                        DROP TABLE wdl_pdw_avar;
                        DROP TABLE wdl_edw_elig;
                        )
                    BY SQLSVR
                ;

            DISCONNECT FROM SQLSVR;

        QUIT;

        PROC DATASETS LIBRARY=myfiles;
            DELETE
                wdl_vrep_:
                edw_elig:
                edw_claims:
            ;
        RUN;

    %enditer: %END; /* End of current client-TPA's complete loop */

%MEND comp_loop;

%MACRO avar_comp(startdt);

    %LET itrdt = %BQUOTE(')%SUBSTR(&startdt., 1, 4)%BQUOTE(-)%SUBSTR(&startdt., 5)%BQUOTE(');

    %comp_loop(&startdt.)
        /* Generates the needed SAS code
           for each client-TPA */

    /* Combine the resulting data */
    DATA myfiles.comp_pdw_edw_&startdt.;
        SET myfiles.pdw_edw_comp_: ;
    RUN;
    
    DATA myfiles.comp_clm_elig_&startdt.;
        SET myfiles.clm_elig_comp_: ;
    RUN;
    
    
    PROC DATASETS LIBRARY=myfiles;
        DELETE
            wdl_vrep_:
            pdw_edw_comp_:
            clm_elig_comp_:
        ;
    RUN;
    
%MEND avar_comp;

/* Loop over the input months */
%MACRO run_avar_comp;

    /* Modify the input start dates
       to use EDW formatting*/
    %LET start_year = %SUBSTR(&start_yearmo., 1, 4);
    %LET start_mo = %SUBSTR(&start_yearmo., 5);
    %LET end_year = %SYSFUNC(YEAR(%SYSFUNC(TODAY())));
    %LET end_mo = %SYSFUNC(MONTH(%SYSFUNC(TODAY())));
    
    /* Generate a list of months to loop through */
    %IF FIND(&start_year., &end_year.) = 0
    %THEN
        %DO;
            %IF &start_year. > &end_year. %THEN %PUT ERROR: Must use an earlier start date.;
            %ELSE %DO;
                
                %LET iter_year = &start_year.;
                
                /* First loop through input start year */
                %DO m = &start_mo. %TO 12;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run analysis on current iterations year-month */
                    %avar_comp(&iter_yearmo.)
                %END;

                %LET iter_year = %EVAL(&iter_year. + 1);
                
                /* Loop through years between start and current years */
                %DO %WHILE (&iter_year. < &end_year.);
                    
                    %DO m = 1 %TO 12;
                        %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                        /* Add a leading 0 to the month if needed
                           (to comply with PDW/EDW yearmonth format) */
                        %IF %LENGTH(&iter_yearmo.) = 5
                        %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                               , 0, %SUBSTR(&iter_yearmo., 5)));
                        %ELSE;
                        
                        /* Run analysis on current iterations year-month */
                        %avar_comp(&iter_yearmo.)
                    %END;

                    %LET iter_year = %EVAL(&iter_year. + 1);

                %END;
                
                /* Loop through the current year */
                %DO m = &start_mo. %TO &end_mo.;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run analysis on current iterations year-month */
                    %avar_comp(&iter_yearmo.)
                %END;
            %END;
        %END;
    %ELSE
        %DO;
            /* Loop through the current years months only */
            %IF &start_mo. > &end_mo. %THEN %PUT ERROR: Must use an earlier start date.;
            %ELSE %DO;
                %DO m = &start_mo. %TO &end_mo.;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run analysis on current iterations year-month */
                    %avar_comp(&iter_yearmo.)
                %END;
            %END;
        %END;

    /* Combined the outputs for each year-month
       into two final datasets */
    DATA myfiles.final_avar_pdw_edw;
        SET myfiles.comp_pdw_edw_: ;
    RUN;
    
    DATA myfiles.final_avar_clm_elig;
        SET myfiles.comp_clm_elig_: ;
    RUN;

    PROC SORT DATA=myfiles.final_avar_pdw_edw;
        BY Client_Name TPA_Name Path monthid;
    RUN;

    PROC SORT DATA=myfiles.final_avar_clm_elig;
        BY Client_Name TPA_Name Path monthid;
    RUN;
    
%MEND;
%run_avar_comp
