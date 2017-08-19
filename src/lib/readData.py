from logs import logDecorator as lD
import json, psycopg2
import pandas as pd

config = json.load(open('../config/config.json'))

@lD.log(config['logging']['logBase'] + '.lib.readData.query')
def query(logger, qry):
    '''simple function that returns the data when the data is small
    
    When there is a small amount of data, then this function is going
    to be useful. This has an internal limit of 10000 for memory
    optimization. If you want larger amounts of data, you may
    want to use a remote curser and iterate over it directly.
    
    Parameters
    ----------
    logger : {logging.Logger}
        logging object 
    query : {str}
        query to be performed
    
    Returns
    -------
    (list of str, list of tuples)
        This is a tuple containing the header information and the 
        data that you are supposed to be returned. If there is a 
        problem, then `(None, None)` is returned
    '''

    try:
        conn = psycopg2.connect(config['db']['mimic_iii']['connection'])
        cur  = conn.cursor('remote cursor')
    except Exception as e:
        logger.error('Unable to connect to the database: {}: \n{}'.format(
            config['db']['mimic_iii']['connection']), str(e))
        return None, None

    data, header = None, None
    maxNum       = 10000
    
    try:
        # Fetch the actual data
        # ----------------------------------
        cur.execute(qry)
        data    = cur.fetchmany(maxNum)
        header  = [desc[0] for desc in cur.description]

        logger.info('Query: \n{}\n  --> returned {} values'.format(qry, len(data)))

    except Exception as e:
        logger.error('Unable to perform the query: {}\n{}'.format(
            qry, str(e)))

    try:
        cur.close()
        conn.close()
    except Error as e:
        logger.error('Unable to close the connection for some reason')

    return header, data


@lD.log(config['logging']['logBase'] + '.lib.readData.queryDf')
def queryDf(logger, qry):
    '''[summary]
    
    [description]
    
    Parameters
    ----------
    logger : {logging.Logger}
        logging object 
    query : {str}
        query to be performed
    
    Returns
    -------
    pandas DataFrame
        A Pandas DataFrame containing the data. If there is an error
        a None is returned
    '''

    header, data = query(qry)
    if header is None:
        return None

    try:
        df = pd.DataFrame(data, columns=header)
    except Exception as e:
        logger.error('Unable to convert the data to a dataframe for query:\n{}\n{}'.format(
            query, str(e)))
        return None


    return df
