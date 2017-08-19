from logs import logDecorator as lD
import json, psycopg2

from lib     import readData
from result1 import exampleAnalysis

config = json.load(open('../config/config.json'))

@lD.log(config['logging']['logBase'] + '.simpleQuery')
def simpleQuery(logger):
    '''example simple query
    
    This function shown how to run a simple query. The logger is 
    injected into the function as the first parameter via the 
    decorator, so there is no need for passing this value.
    
    Parameters
    ----------
    logger : {logging.Logger instance}
        Logger that will log info and errors. 
    
    Returns
    -------
    [list of tuples]
        This is the result of the query that we just performed.
    '''

    try:
        conn = psycopg2.connect(config['db']['mimic_iii']['connection'])
        cur = conn.cursor()
    except Exception as e:
        logger.error('Unable to connect to the database: {}: \n{}'.format(
            config['db']['mimic_iii']['connection']), str(e))
        return

    try:
        data = None
        query = '''
            select 
                * 
            from 
                raw_data.callout
            where
                callout_service == 'CCU'
            limit 100
        '''
        cur.execute(query)
        data = cur.fetchall()
        logger.info('Query: \n{}\n  --> returned {} values'.format(query, len(data)))
    except Exception as e:
        logger.error('Unable to perform the query: {}\n{}'.format(
            query, str(e)))

    try:
        cur.close()
        conn.close()
    except Error as e:
        logger.error('Unable to close the connection for some reason')

    return data

@lD.logInit(config['logging']['logBase'])
def main(logger):

    if False:
        # These are analyses that have already been
        # performed
        # -----------------------------------------
        try: print(simpleQuery())
        except Exception as e: 
            logger.error('Unable to do simpleQuery()\n{}'.format(str(e)))

    if True:
        # These are analysis that we are yet to do
        # -----------------------------------------
        try: exampleAnalysis.someStuff()
        except Exception as e: 
            logger.error('Unable to do exampleAnalysis.someStuff: \n{}'.format(str(e)))
        
        
 

    return

if __name__ == '__main__':
    main()


