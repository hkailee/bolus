import os, json, psycopg2

import numpy             as np
import pandas            as pd
import matplotlib.pyplot as plt

from lib  import readData
from logs import logDecorator as lD

config = json.load(open('../config/config.json'))
folder = '../results/result1'

@lD.log(config['logging']['logBase'] + '.result1.someStuff')
def someStuff(logger):
    '''just a simple function to show an example analysis
    
    This is there just to show how an analysis might
    be performed to produce the value of a result in 
    the `../results/result1` folder.
    '''

    query = '''
    select 
        admission_type, insurance, count(*)
    from 
        raw_data.admissions
    group by
        admission_type, insurance;
    '''
    result = readData.queryDf(query)

    if result is None: return

    # Make the folder
    if not os.path.exists('../results/result1'):
        os.makedirs('../results/result1')

    result.sort_values('admission_type', inplace=True)
    print(result)

    # Save the data
    result.to_csv(os.path.join(folder, 'someStuff.csv'), index=False)

    for i, (ins, df) in enumerate(result.groupby('insurance')):
        print(df)
        plt.bar(np.arange(len(df)) + i*0.15 , df['count'], width=0.1, label=ins)

    xVals = df.admission_type.values
    plt.xticks(np.arange(len(xVals))+0.3, xVals)

    plt.legend()
    plt.yscale('log')
    plt.savefig(os.path.join(folder, 'someStuff.png'))
    plt.close('all')


    return
