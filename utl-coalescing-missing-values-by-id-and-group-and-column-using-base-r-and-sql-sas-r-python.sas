%let pgm=utl-coalescing-missing-values-by-id-and-group-and-column-using-base-r-and-sql-sas-r-python;

%stop_submission;

Colalescing missing values by id and group and column using base r and sql sas r python

    CONTENTS

       1 sas sql
       2 base r
       3 r sql
       4 python sql
       5 just insert the sql code in
         see
         https://tinyurl.com/4e6yaap8

Caveats:
 Only one non-missing value possible for each columm within group id sets

github
https://tinyurl.com/733hmzha
https://github.com/rogerjdeangelis/utl-coalescing-missing-values-by-id-and-group-and-column-using-base-r-and-sql-sas-r-python

stackoverflow
https://tinyurl.com/ek6mevxa
https://stackoverflow.com/questions/79543720/in-r-how-can-i-collapse-the-data-of-grouped-rows-into-one-row


/******************************************************************************************************************************/
/*       INPUT                          |    PROCESS                                |           OUTPUT                        */
/*       =====                          |    =======                                |           ======                        */
/*                                      |                                           |                                         */
/*                        Collase when  | 1 SAS SQL                                 |ID GRP MAX_A MAX_B MAX_C MAX_D           */
/* ======                 group size>1  | self explanatory                          |                                         */
/* ID GRP  A   B    C   D               |                                           | 1  .    10    25     .     . collapsed  */
/*                                      |                                           |                                         */
/*  1  .  10   .    .   . same id group |                                           | 1  2     .     .    23     .            */
/*  1  .   .  25    .   . so collapse   | proc sql;                                 | 2  .    20     .     .    25            */
/*                                      |  create                                   | 2  1    25    35    45    25            */
/*  1  2   .   .   23   . new id group  |     table want as                         | 3  .    30    15    20    30 collapsed  */
/*                                      |  select                                   | 3  2     .    15    20    30            */
/*  2  .  20   .    .  25 new id group  |     id                                    |                                         */
/*                                      |    ,grp                                   |                                         */
/*  2  1  25  35   45  25 new id group  |    ,max(a) as max_a                       |                                         */
/*                                      |    ,max(b) as max_b                       |                                         */
/*  3  2   .  15   20  30 new id group  |    ,max(c) as max_c                       |                                         */
/*                                      |    ,max(d) as max_d                       |                                         */
/*  3  .  30   .    .   . same id group |  from                                     |                                         */
/*  3  .   .  15   20  30 so collapse   |    sd1.have                               |                                         */
/*                                      |  group                                    |                                         */
/* options validvarname=upcase;         |    by id, grp                             |                                         */
/* libname sd1 "d:/sd1";                | ;quit;                                    |                                         */
/* data sd1.have;                       |                                           |                                         */
/*   input id  grp A  B  C  D;          |-------------------------------------------------------------------------------------*/
/* cards4;                              |                                           |                                         */
/* 1 . 10 . . .                         | 2 BASE R                                  | R                                       */
/* 1 . . 25 . .                         | ========                                  |   ID GRP  A  B  C  D                    */
/* 1 2 . . 23 .                         |                                           | 2  1   2 NA NA 23 NA                    */
/* 2 . 20 . . 25                        | %utl_rbeginx;                             | 4  1  NA 10 25 NA NA                    */
/* 2 1 25 35 45 25                      | parmcards4;                               | 1  2   1 25 35 45 25                    */
/* 3 2 . 15 20 30                       | library(haven)                            | 5  2  NA 20 NA NA 25                    */
/* 3 . 30 . . .                         | source("c:/oto/fn_tosas9x.R")             | 3  3   2 NA 15 20 30                    */
/* 3 . . 15 20 30                       | have<-read_sas(                           | 6  3  NA 30 15 20 30                    */
/* ;;;;                                 |   "d:/sd1/have.sas7bdat")                 |                                         */
/* run;quit;                            | print(have)                               | SAS                                     */
/*                                      | want<-                                    | ID GRP    A     B     C     D           */
/*                                      |  sort_by(                                 |                                         */
/*                                      |   aggregate(.~ID+factor(                  |  1  .    10    25     .     .           */
/*                                      |     GRP                                   |                                         */
/*                                      |    ,exclude=NULL)                         |  1  2     .     .    23     .           */
/*                                      |    ,have                                  |  2  .    20     .     .    25           */
/*                                      |    ,\(x) ifelse(all(is.na(x))             |  2  1    25    35    45    25           */
/*                                      |       ,NA                                 |  3  .    30    15    20    30           */
/*                                      |       ,max(x, na.rm = TRUE))              |  3  2     .    15    20    30           */
/*                                      |       ,na.action = na.pass                |                                         */
/*                                      |   )[names(have)]                          |                                         */
/*                                      |  ,~ID                                     |                                         */
/*                                      | )                                         |                                         */
/*                                      | want                                      |                                         */
/*                                      | fn_tosas9x(                               |                                         */
/*                                      |       inp    = want                       |                                         */
/*                                      |      ,outlib ="d:/sd1/"                   |                                         */
/*                                      |      ,outdsn ="want"                      |                                         */
/*                                      |      )                                    |                                         */
/*                                      | ;;;;                                      |                                         */
/*                                      | %utl_rendx;                               |                                         */
/*                                      |                                           |                                         */
/*                                      |-------------------------------------------------------------------------------------*/
/*                                      |                                           |                                         */
/*                                      | 3 R SQL                                   | > want                                  */
/*                                      | =======                                   |   ID GRP max_a max_b max_c max_d        */
/*                                      |                                           | 1  1  NA    10    25    NA    NA        */
/*                                      | %utl_rbeginx;                             | 2  1   2    NA    NA    23    NA        */
/*                                      | parmcards4;                               | 3  2  NA    20    NA    NA    25        */
/*                                      | library(haven)                            | 4  2   1    25    35    45    25        */
/*                                      | library(sqldf)                            | 5  3  NA    30    15    20    30        */
/*                                      | source("c:/oto/fn_tosas9x.R")             | 6  3   2    NA    15    20    30        */
/*                                      | have<-read_sas(                           |                                         */
/*                                      |  "d:/sd1/have.sas7bdat")                  | SAS                                     */
/*                                      | print(have)                               | ID GRP MAX_A MAX_B MAX_C MAX_D          */
/*                                      | want<-sqldf('                             |                                         */
/*                                      |  select                                   |  1  .    10    25     .     .           */
/*                                      |     id                                    |                                         */
/*                                      |    ,grp                                   |  1  2     .     .    23     .           */
/*                                      |    ,max(a) as max_a                       |  2  .    20     .     .    25           */
/*                                      |    ,max(b) as max_b                       |  2  1    25    35    45    25           */
/*                                      |    ,max(c) as max_c                       |  3  .    30    15    20    30           */
/*                                      |    ,max(d) as max_d                       |  3  2     .    15    20    30           */
/*                                      |  from                                     |                                         */
/*                                      |    have                                   |                                         */
/*                                      |  group                                    |                                         */
/*                                      |    by id, grp                             |                                         */
/*                                      |  ')                                       |                                         */
/*                                      | want                                      |                                         */
/*                                      | fn_tosas9x(                               |                                         */
/*                                      |       inp    = want                       |                                         */
/*                                      |      ,outlib ="d:/sd1/"                   |                                         */
/*                                      |      ,outdsn ="want"                      |                                         */
/*                                      |      )                                    |                                         */
/*                                      | ;;;;                                      |                                         */
/*                                      | %utl_rendx;                               |                                         */
/*                                      |                                           |                                         */
/*                                      | proc print data=sd1.want;                 |                                         */
/*                                      | run;quit;                                 |                                         */
/*                                      |                                           |                                         */
/*                                      |-------------------------------------------------------------------------------------*/
/*                                      |                                           |                                         */
/*                                      | 4 python sql                              |                                         */
/*                                      | ============                              |     ID  GRP  max_a  max_b  max_c  max_d */
/*                                      |                                           | 0  1.0  NaN   10.0   25.0    NaN    NaN */
/*                                      | %utl_pybeginx;                            | 1  1.0  2.0    NaN    NaN   23.0    NaN */
/*                                      | parmcards4;                               | 2  2.0  NaN   20.0    NaN    NaN   25.0 */
/*                                      | exec(open('c:/oto/fn_python.py').read()); | 3  2.0  1.0   25.0   35.0   45.0   25.0 */
/*                                      | have,meta =  \                            | 4  3.0  NaN   30.0   15.0   20.0   30.0 */
/*                                      |  ps.read_sas7bdat('d:/sd1/have.sas7bdat');| 5  3.0  2.0    NaN   15.0   20.0   30.0 */
/*                                      | want=pdsql('''                            |                                         */
/*                                      |  select                \                  | SAS                                     */
/*                                      |     id                 \                  | ID GRP MAX_A MAX_B MAX_C MAX_D          */
/*                                      |    ,grp                \                  |                                         */
/*                                      |    ,sum(a) as max_a    \                  |  1  .    10    25     .     .           */
/*                                      |    ,sum(b) as max_b    \                  |                                         */
/*                                      |    ,sum(c) as max_c    \                  |  1  2     .     .    23     .           */
/*                                      |    ,sum(d) as max_d    \                  |  2  .    20     .     .    25           */
/*                                      |  from                  \                  |  2  1    25    35    45    25           */
/*                                      |    have                \                  |  3  .    30    15    20    30           */
/*                                      |  group                 \                  |  3  2     .    15    20    30           */
/*                                      |    by id, grp          \                  |                                         */
/*                                      | ''')                                      |                                         */
/*                                      | print(want);                              |                                         */
/*                                      | fn_tosas9x(want        \                  |                                         */
/*                                      |     ,outlib='d:/sd1/'  \                  |                                         */
/*                                      |     ,outdsn='pywant');                    |                                         */
/*                                      | ;;;;                                      |                                         */
/*                                      | %utl_pyendx;                              |                                         */
/******************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  input id  grp A  B  C  D;
cards4;
1 . 10 . . .
1 . . 25 . .
1 2 . . 23 .
2 . 20 . . 25
2 1 25 35 45 25
3 2 . 15 20 30
3 . 30 . . .
3 . . 15 20 30
;;;;
run;quit;

/**************************************************************************************************************************/
/* ID    GRP     A     B     C     D                                                                                      */
/*                                                                                                                        */
/*  1     .     10     .     .     .                                                                                      */
/*  1     .      .    25     .     .                                                                                      */
/*  1     2      .     .    23     .                                                                                      */
/*  2     .     20     .     .    25                                                                                      */
/*  2     1     25    35    45    25                                                                                      */
/*  3     2      .    15    20    30                                                                                      */
/*  3     .     30     .     .     .                                                                                      */
/*  3     .      .    15    20    30                                                                                      */
/**************************************************************************************************************************/

/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/
proc sql;
 create
    table want as
 select
    id
   ,grp
   ,max(a) as max_a
   ,max(b) as max_b
   ,max(c) as max_c
   ,max(d) as max_d
 from
   sd1.have
 group
   by id, grp
;quit;

/**************************************************************************************************************************/
/* ID    GRP    MAX_A    MAX_B    MAX_C    MAX_D                                                                          */
/*                                                                                                                        */
/*  1     .       10       25        .        .                                                                           */
/*  1     2        .        .       23        .                                                                           */
/*  2     .       20        .        .       25                                                                           */
/*  2     1       25       35       45       25                                                                           */
/*  3     .       30       15       20       30                                                                           */
/*  3     2        .       15       20       30                                                                           */
/**************************************************************************************************************************/


/*___    _
|___ \  | |__   __ _ ___  ___   _ __
  __) | | `_ \ / _` / __|/ _ \ | `__|
 / __/  | |_) | (_| \__ \  __/ | |
|_____| |_.__/ \__,_|___/\___| |_|

*/

proc datasets lib=sd1 nolist nodetails;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(haven)
source("c:/oto/fn_tosas9x.R")
have<-read_sas(
  "d:/sd1/have.sas7bdat")
print(have)
want<-
 sort_by(
  aggregate(.~ID+factor(
    GRP
   ,exclude=NULL)
   ,have
   ,\(x) ifelse(all(is.na(x))
      ,NA
      ,max(x, na.rm = TRUE))
      ,na.action = na.pass
  )[names(have)]
 ,~ID
)
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/* R                   |  SAS                                                                                             */
/*  D GRP  A  B  C  D  |  ROWNAMES    ID    GRP     A     B     C     D                                                   */
/*                     |                                                                                                  */
/*  1   2 NA NA 23 NA  |      2        1     2      .     .    23     .                                                   */
/*  1  NA 10 25 NA NA  |      4        1     .     10    25     .     .                                                   */
/*  2   1 25 35 45 25  |      1        2     1     25    35    45    25                                                   */
/*  2  NA 20 NA NA 25  |      5        2     .     20     .     .    25                                                   */
/*  3   2 NA 15 20 30  |      3        3     2      .    15    20    30                                                   */
/*  3  NA 30 15 20 30  |      6        3     .     30    15    20    30                                                   */
/**************************************************************************************************************************/

/*____                    _
|___ /   _ __   ___  __ _| |
  |_ \  | `__| / __|/ _` | |
 ___) | | |    \__ \ (_| | |
|____/  |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
have<-read_sas(
 "d:/sd1/have.sas7bdat")
print(have)
want<-sqldf('
 select
    id
   ,grp
   ,max(a) as max_a
   ,max(b) as max_b
   ,max(c) as max_c
   ,max(d) as max_d
 from
   have
 group
   by id, grp
 ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/* R                                 |  SAS                                                                               */
/*   ID GRP max_a max_b max_c max_d  |  ROWNAMES    ID    GRP    MAX_A    MAX_B    MAX_C    MAX_D                         */
/*                                   |                                                                                    */
/* 1  1  NA    10    25    NA    NA  |      1        1     .       10       25        .        .                          */
/* 2  1   2    NA    NA    23    NA  |      2        1     2        .        .       23        .                          */
/* 3  2  NA    20    NA    NA    25  |      3        2     .       20        .        .       25                          */
/* 4  2   1    25    35    45    25  |      4        2     1       25       35       45       25                          */
/* 5  3  NA    30    15    20    30  |      5        3     .       30       15       20       30                          */
/* 6  3   2    NA    15    20    30  |      6        3     2        .       15       20       30                          */
/**************************************************************************************************************************/

/*  _                 _   _                             _
| || |    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _| | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|   | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
         |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;
 delete pywant;
run;quit;

%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_python.py').read());
have,meta =  \
 ps.read_sas7bdat('d:/sd1/have.sas7bdat');
want=pdsql('''
 select                \
    id                 \
   ,grp                \
   ,sum(a) as max_a    \
   ,sum(b) as max_b    \
   ,sum(c) as max_c    \
   ,sum(d) as max_d    \
 from                  \
   have                \
 group                 \
   by id, grp          \
''')
print(want);
fn_tosas9x(want        \
    ,outlib='d:/sd1/'  \
    ,outdsn='pywant');
;;;;
%utl_pyendx;


proc print data=sd1.pywant;
run;quit;

/**************************************************************************************************************************/
/*  R                                         |  SAS                                                                      */
/*      ID  GRP  max_a  max_b  max_c  max_d   |  ID    GRP    MAX_A    MAX_B    MAX_C    MAX_D                            */
/*                                            |                                                                           */
/*  0  1.0  NaN   10.0   25.0    NaN    NaN   |   1     .       10       25        .        .                             */
/*  1  1.0  2.0    NaN    NaN   23.0    NaN   |   1     2        .        .       23        .                             */
/*  2  2.0  NaN   20.0    NaN    NaN   25.0   |   2     .       20        .        .       25                             */
/*  3  2.0  1.0   25.0   35.0   45.0   25.0   |   2     1       25       35       45       25                             */
/*  4  3.0  NaN   30.0   15.0   20.0   30.0   |   3     .       30       15       20       30                             */
/*  5  3.0  2.0    NaN   15.0   20.0   30.0   |   3     2        .       15       20       30                             */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
