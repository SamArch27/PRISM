#include "duckdb.hpp"
#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
// #include "udf.hpp"

duckdb::DuckDB main_db(nullptr);

double mean(std::vector<double> v){
    double sum = 0;
    for(int i:v){
        sum += i;
    }
    return sum/v.size();
}

// void prepare_env(duckdb::Connection *con){
//     con->Query("CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL, \
//                         N_NAME       CHAR(25) NOT NULL, \
//                         N_REGIONKEY  INTEGER NOT NULL, \
//                         N_COMMENT    VARCHAR(152));\
// \
// CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL, \
//                         R_NAME       CHAR(25) NOT NULL, \
//                         R_COMMENT    VARCHAR(152)); \
// \
// CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL, \
//                       P_NAME        VARCHAR(55) NOT NULL,  \
//                       P_MFGR        CHAR(25) NOT NULL, \
//                       P_BRAND       CHAR(10) NOT NULL, \
//                       P_TYPE        VARCHAR(25) NOT NULL, \
//                       P_SIZE        INTEGER NOT NULL, \
//                       P_CONTAINER   CHAR(10) NOT NULL, \
//                       P_RETAILPRICE DECIMAL(15,2) NOT NULL, \
//                       P_COMMENT     VARCHAR(23) NOT NULL ); \
// \
// CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL, \
//                          S_NAME        CHAR(25) NOT NULL, \
//                          S_ADDRESS     VARCHAR(40) NOT NULL, \
//                          S_NATIONKEY   INTEGER NOT NULL, \
//                          S_PHONE       CHAR(15) NOT NULL, \
//                          S_ACCTBAL     DECIMAL(15,2) NOT NULL, \
//                          S_COMMENT     VARCHAR(101) NOT NULL); \
// \
// CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL, \
//                          PS_SUPPKEY     INTEGER NOT NULL, \
//                          PS_AVAILQTY    INTEGER NOT NULL, \
//                          PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL, \
//                          PS_COMMENT     VARCHAR(199) NOT NULL ); \
// \
// CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL, \
//                          C_NAME        VARCHAR(25) NOT NULL, \
//                          C_ADDRESS     VARCHAR(40) NOT NULL, \
//                          C_NATIONKEY   INTEGER NOT NULL, \
//                          C_PHONE       CHAR(15) NOT NULL, \
//                          C_ACCTBAL     DECIMAL(15,2)   NOT NULL, \
//                          C_MKTSEGMENT  CHAR(10) NOT NULL, \
//                          C_COMMENT     VARCHAR(117) NOT NULL); \
// \
// CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL, \
//                        O_CUSTKEY        INTEGER NOT NULL, \
//                        O_ORDERSTATUS    CHAR(1) NOT NULL, \
//                        O_TOTALPRICE     DECIMAL(15,2) NOT NULL, \
//                        O_ORDERDATE      DATE NOT NULL, \
//                        O_ORDERPRIORITY  CHAR(15) NOT NULL,  \
//                        O_CLERK          CHAR(15) NOT NULL, \
//                        O_SHIPPRIORITY   INTEGER NOT NULL, \
//                        O_COMMENT        VARCHAR(79) NOT NULL); \
// \
// CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,\
//                          L_PARTKEY     INTEGER NOT NULL,\
//                          L_SUPPKEY     INTEGER NOT NULL,\
//                          L_LINENUMBER  INTEGER NOT NULL,\
//                          L_QUANTITY    INTEGER NOT NULL,\
//                          L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,\
//                          L_DISCOUNT    DECIMAL(15,2) NOT NULL,\
//                          L_TAX         DECIMAL(15,2) NOT NULL,\
//                          L_RETURNFLAG  CHAR(1) NOT NULL,\
//                          L_LINESTATUS  CHAR(1) NOT NULL,\
//                          L_SHIPDATE    DATE NOT NULL,\
//                          L_COMMITDATE  DATE NOT NULL,\
//                          L_RECEIPTDATE DATE NOT NULL,\
//                          L_SHIPINSTRUCT CHAR(25) NOT NULL,\
//                          L_SHIPMODE     CHAR(10) NOT NULL,\
//                          L_COMMENT      VARCHAR(44) NOT NULL);");
//     con->Query("INSERT INTO NATION (SELECT * FROM read_csv_auto('dataset/nation.tbl'))");
//     con->Query("INSERT INTO REGION (SELECT * FROM read_csv_auto('dataset/region.tbl'))");
//     con->Query("INSERT INTO PART (SELECT * FROM read_csv_auto('dataset/part.tbl'))");
//     con->Query("INSERT INTO SUPPLIER (SELECT * FROM read_csv_auto('dataset/supplier.tbl'))");
//     con->Query("INSERT INTO PARTSUPP (SELECT * FROM read_csv_auto('dataset/partsupp.tbl'))");
//     con->Query("INSERT INTO CUSTOMER (SELECT * FROM read_csv_auto('dataset/customer.tbl'))");
//     con->Query("INSERT INTO ORDERS (SELECT * FROM read_csv_auto('dataset/orders.tbl'))");
//     con->Query("INSERT INTO LINEITEM (SELECT * FROM read_csv_auto('dataset/lineitem.tbl'))");
//     con->CreateVectorizedFunction("isListDistinct", {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::BOOLEAN, &isListDistinct);
// }

static const int EPOCH_NUM = 1;
int main(int argc, char const *argv[])
{
    duckdb::Connection con(main_db);
    std::cout << "Preparing environment..." << std::endl;
    // prepare_env(&con);
    std::cout << "Running Query..." << std::endl;
    std::chrono::time_point<std::chrono::steady_clock> start;
    std::chrono::time_point<std::chrono::steady_clock> end;
    //========== Test 1 ===========
    std::vector<double> time1;
    for(int i=0;i<EPOCH_NUM;i++){
        start = std::chrono::steady_clock::now();
        auto result1 = con.Query("SELECT concat(col0, ',') from (values ('1,2,3,4,5,2'), ('1,143,2'), ('1,,'))");
        // auto result1 = con.Query("select l_shipdate + 1 from lineitem");
        end = std::chrono::steady_clock::now();
        if(result1->HasError())
            std::cout << result1->GetError() << std::endl;
        result1->Print();
        time1.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    }
    std::cout << "Time 1: " << mean(time1) << " ms" << std::endl;
    
    return 0;
}
