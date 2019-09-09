class SqlQueries:
    drop_oltp_tables = """
        DROP TABLE IF EXISTS ports;
        DROP TABLE IF EXISTS countries;
        DROP TABLE IF EXISTS visa_codes;
        DROP TABLE IF EXISTS demographics;
        DROP TABLE IF EXISTS i94;
    """
    create_oltp_tables = """
        CREATE TABLE IF NOT EXISTS ports (
            port_id     varchar(3)   NOT NULL primary key,
            name        varchar(100) NOT NULL,
            city        varchar(100) NOT NULL,
            state       varchar(100) NOT NULL
        ) diststyle all;
        
        
        CREATE TABLE IF NOT EXISTS countries (
            country_id      smallint     NOT NULL primary key,
            name            varchar(100) NOT NULL
        ) diststyle all;
        
        CREATE TABLE IF NOT EXISTS visa_codes (
            visa_id varchar(3) NOT NULL primary key,
            visa_name varchar(100) NOT NULL
        ) diststyle all;
        

        CREATE TABLE IF NOT EXISTS demographics (
            demographics_id          varchar(100)     NOT NULL primary key,
            port_id                  varchar(3)       NOT NULL distkey,  
            city                     varchar(100)     ,
            state                    varchar(100)     ,  
            median_age               numeric(4,1)     ,
            male_population          int              ,
            female_population        int              ,
            total_population         int              ,
            avg_household_size       numeric(3,2)     ,
            foreign_born             int              ,
            race                     varchar(100)     ,
            race_code                varchar(3)       
        ) diststyle key;
        
        CREATE TABLE IF NOT EXISTS i94 (
            cic_id            int         NOT NULL primary key,
            port_id          varchar(3)   NOT NULL distkey,
            visa_id          varchar(3)   NOT NULL,
            cit_id           smallint     ,
            res_id           smallint     ,
            year             smallint     ,
            month            smallint     ,
            age              smallint     ,
            gender           varchar(2)   ,
            arrival_date      date        ,
            depart_date      date         ,
            date_begin       date         ,
            date_end         date       
        ) diststyle key;
    
    """
