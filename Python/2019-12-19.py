# -*- coding: utf-8 -*-
"""
Spyder Editor

This is the master table script file
"""
#
import csv
import os
import operator
import tempfile
import time
from datetime import datetime
import argparse
import cProfile

try:
    import pyodbc
except ImportError:
    pass
#
# other functions
#

def dir_from_date(d_d, s_s, w_d):
    """
    check for presence and create directories
    """
    dirdate = ''
    if s_s == 'y':
        dirdate = str(time.strptime(d_d, "%Y-%m-%d")[0])
    else:
        dirdate = str(time.strptime(d_d, "%Y-%m-%d")[0]\
                      +'-' +str(time.strptime(d_d, "%Y-%m-%d")[1]))
    dirname = os.path.join(w_d, dirdate)
    if not os.path.isdir(dirname):
        try:
            os.mkdir(dirname)
        except OSError:
            print('\n\ncreation of the directory %s failed' % dirname, datetime.now())

    return dirname


def create_titles(s_s):
    """
    title string
    """
    title_str = []
    for t_t in s_s:
        title_str.append(t_t)
    return title_str


def ntradingdays():
    """
    number of trading days

    TODO(Art): Replace this with something like -
    pd.date_range('2009-01-01', datetime.datetime.now(), freq=pd.tseries.offsets.BDay())
    """
    return 252*10

def main(working_dir_, source_date_):
    """
    The parametrized main function for CLI in the cloud
    """
    #
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments
#    cwd = os.path.realpath(os.path.dirname(__file__)) #os.getcwd() # ./
    working_dir = working_dir_
    date_ = source_date_
    database = 'qai'
    server = 'cd5m7wkqacpdeus2mia12301.public.dabc3424290b.database.windows.net,3342'
    username = 'a123.joe.petviashvili'
    password = '9tdnLh%rm#K51!HW'
#Authentication: SQL Server Authentication
    # NOTE: The following works on a Mac with the MSSQL 13 driver installed - it is here as the
    # default because Art's Anaconda environment doesn't show a non-empty list of drivers from
    # pyodbc
    driver = '/usr/local/lib/libmsodbcsql.13.dylib' # '{ODBC Driver 13 for SQL Server}'
    drivers = [item for item in pyodbc.drivers()]
    if drivers:
        driver = drivers[0]
    #print('driver:{}'.format(driver))
    #
    cnxn = pyodbc.connect('DRIVER=' + driver +
                          ';SERVER=' + server +
                          ';PORT=1433;DATABASE=' + database +
                          ';UID=' + username +
                          ';PWD=' + password)
    cursor_ = cnxn.cursor()

    print('\n\ndownloading corporate events data ... ', datetime.now())
    print('\n\nprocessing ...', datetime.now())
    query = '''DECLARE @N_HYPOTH_SHARES    INT     =   100;

SELECT DISTINCT
        FORMAT (C.EffectiveDate, 'd', 'en-us') as EffectiveDate,
        K.TICKER,
 --       A.SecCode,
        A.Name,
      --  C.ActionTypeCode,
        P.Desc_,
        C.NumNewShares,
        C.NumOldShares,
        FORMAT (C.AnnouncedDate, 'd', 'en-us') as AnnouncedDate,
        FORMAT (C.RecordDate, 'd', 'en-us') as RecordDate,
        FORMAT (C.ExpiryDate, 'd', 'en-us') as ExpiryDate,
        C.OfferCmpyName,
        C.CashAmt,
    --    D.RIC,


                @N_HYPOTH_SHARES    AS  NumHypothShares,

                CASE    WHEN
                                C.NumOldShares != 0
                        THEN
                                floor(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares)
                        ELSE
                                0
                        END     AS  NumSharesFinal,

                CASE    WHEN
                                C.NumOldShares != 0
                        THEN
                                @N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares-floor(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares)
                        ELSE
                                0
                        END     AS  LeftOverShares,

                CASE    WHEN
                                C.NumOldShares != 0
                        THEN
                                Q.Close_*(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares-floor(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares))
                        ELSE
                                0
                        END     AS  CashInLieu,

                CASE    WHEN
                                C.NumOldShares != 0
                        THEN    
                            CASE
                                WHEN
                                    C.NumNewShares = 0
                                THEN
                                    @N_HYPOTH_SHARES*C.CashAmt
                                ELSE
                                    @N_HYPOTH_SHARES*C.CashAmt+Q.Close_*(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares-floor(@N_HYPOTH_SHARES*C.NumNewShares/C.NumOldShares))
                                END
                        ELSE
                                C.CashAmt
                        END     AS  CashFinal

    --    FORMAT (C.EffectiveDate, 'd', 'en-us') as EffectiveDate

        FROM SecMSTRX      A

        JOIN SecMapX       B
            ON A.SECCODE = B.SECCODE
            AND A.TYPE_ = 1
            AND B.VENTYPE = 33

        JOIN    DS2CapEvent     C
            ON      B.Vencode = C.InfoCode

        JOIN    DS2xRef                P 
            ON      C.ActionTypeCode = P.code

        JOIN    RDCSecMapX      M
            ON      A.SecCode = M.SecCode

        JOIN    RDCQuoteInfo    K
            ON      M.VenCode = K.QuoteID

        JOIN    RDCRICData      D
            ON      K.QuoteID = D.QuoteID
        
        JOIN    Ds2PrimqtPrc     Q
            ON      Q.infocode = C.infocode

         WHERE
            EffectiveDate <= datediff(d, 0, getdate())
            AND
            EffectiveDate >=\''''+date_+''''
        --    AND
        --    C.ActionTypeCode='MERG'
            AND 
            Q.MarketDate=EffectiveDate

  --      ORDER BY C.EffectiveDate DESC
'''
    with open(os.path.join(working_dir, 'query_corporate_events.txt'), 'w') as query_file:
        query_file.write(query)
        keep_trying_to_query = True
        result = None
# the query might fail because the computer got moved to a different location,
# which resulted in IP change; in this case, try to re-open the connection, then re-do the query
        while keep_trying_to_query:
            try:
                print('\n\ntrying to execute cursor_.execute(COMPOSED_query)...',
                      datetime.now())
                cursor_.execute(query)
                try:
                    print('\n\ntrying to execute result = cursor_.fetchall()...',
                          datetime.now())
                    result = cursor_.fetchall()
                    keep_trying_to_query = False
                except Exception as err:
                    try:
                        print('\n\nexception #5 for cursor_.execute(COMPOSED_query)',
                              err, datetime.now())
                        print('\n\nexception #6 for result = cursor_.fetchall()',
                              err, datetime.now())
                        cursor_.close()
                        cnxn.close()
                        print("\n\nre-opening server connection...", datetime.now())
                        cnxn = pyodbc.connect('DRIVER='+driver+
                                              ';SERVER='+server+
                                              ';PORT=1433;DATABASE='+database+
                                              ';UID='+username+
                                              ';PWD='+password)
                        cursor_ = cnxn.cursor()
                    except Exception as err:
                        print('\n\nexception #7 for reconnect', err, datetime.now())
            except Exception as err:
                try:
                    print('\n\nexception #8 for cursor_.execute(COMPOSED_query)',
                          err, datetime.now())
                    print('\n\nexception #9 for result = cursor_.fetchall()',
                          err, datetime.now())
                    cursor_.close()
                    cnxn.close()
                    print("\n\nre-opening server connection...", datetime.now())
                    cnxn = pyodbc.connect('DRIVER='+driver+
                                          ';SERVER='+server+
                                          ';PORT=1433;DATABASE='+database+
                                          ';UID='+username+
                                          ';PWD='+password)
                    cursor_ = cnxn.cursor()
                except Exception as err:
                    print('\n\nexception #10 for reconnect', err, datetime.now())
#
        if result is not None:
            table1 = []
            table1.append(create_titles([
                'EffectiveDate'
                , 'Ticker'
                , 'ComName'
                , 'ActionTypeCode'
                , 'NumNewShares'
                , 'NumOldShares'
                , 'AnnouncedDate'
                , 'RecordDate'
                , 'ExpiryDate'
                , 'OfferCmpyName'
                , 'CashAmt'
                , 'NumHypothShares'
                , 'NumSharesFinal'
                , 'LeftOverShares'
                , 'CashInLieu'
                , 'CashFinal'
                ]))
            table = []
            print("\n\nquery produced %d rows" % len(result), datetime.now())
            for row in result:
                row3 = []
#   Eff date            -- 0
#   Ticker              -- 1
#   CO.NAME             -- 2
#   C.ActionType        -- 3
#   C.NumNewShares      -- 4
#   C.NumOldShares      -- 5
#   C.AnnouncedDate     -- 6
#   C.RecordDate        -- 7
#   C.ExpiryDate        -- 8
#   C.OfferCmpyName     -- 9
#   Cash amount         -- 10
#   NumHypothShares     -- 11
#   NumSharesFinal      -- 11
#   LeftOverShares      -- 12
#   CashInLieu          -- 13
#   CashFinal           -- 14
#
#                itemp = 0
                for itemp in range(16):
                    if row[itemp] is not None:
                        row3.append(row[itemp])
                    else:
                        row3.append('')

#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                    row3.append('')
#                itemp += 1
#                if row[itemp] is not None:
#                    row3.append(row[itemp])
#                else:
#                   row3.append('')
#
                if row3 not in table:
                    table.append(row3)
#
            table = sorted(table, key=operator.itemgetter(1))
            table = sorted(table, key=operator.itemgetter(0), reverse=True)
            table2 = []
            for row in table:
                table2.append(row)
            table1 += table2
            now = datetime.now()
            ofp = os.path.join(working_dir, 'corporate_events_data_'
                               + now.strftime("%Y_%m_%d")+'.csv')
            with open(ofp, 'w') as result_file:
                w_r = csv.writer(result_file, dialect='excel')
                w_r.writerows(table1)
#
    print('\n\nexiting ... ', datetime.now())


def is_valid_date_string(maybe_date_string: str) -> str:
    """
    Errors out if maybe_date_string is not a valid date string (as expected by this
    script). Otherwise, simply returns the string.
    """
    time.strptime(maybe_date_string, '%Y-%m-%d')
    return maybe_date_string

if __name__ == '__main__':
    PR = cProfile.Profile()
    PR.enable()
#
    PARSER = argparse.ArgumentParser(description='Input parameters: ')
    PARSER.add_argument(
        '-d',
        '--working-directory',
        type=str, # make it start with ./ and end with /
        default=None,
        help='call Art at 409-443-4701, he\'ll refer you to Neeraj'
    )
    PARSER.add_argument(
        '-s',
        '--source-date',
        type=str,
        default=str(datetime.now()),
        help='the date from which the timestamping begins'
    )

    ARGS = PARSER.parse_args()

    WORKING_DIRECTORY = (
        ARGS.working_directory if ARGS.working_directory is not None else tempfile.mkdtemp()
    )
    SOURCE_DATE = ARGS.source_date

    print(f'Working directory: {WORKING_DIRECTORY}')
    print(f'Source date: {SOURCE_DATE}')

    main(
        WORKING_DIRECTORY,
        SOURCE_DATE
    )
    PR.disable()
#    PR.print_stats()
