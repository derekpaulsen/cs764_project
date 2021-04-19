import pandas as pd
import sys
import numpy as np
from argparse import ArgumentParser

argp = ArgumentParser()
argp.add_argument('--n', required=True, type=int)
argp.add_argument('--nreads', required=True, type=int)



OUTPUT_DIR = './workload/'
RAND_MIN = 0
RAND_MAX = 2**30
READ = 'READ'
INSERT='INSERT'

def write_out(df, name):
    df = df[['type', 'key']]
    df.to_csv(OUTPUT_DIR + name, index=False, header=False, sep='\t')


def generate_seq_insert(n):
    
    df = pd.DataFrame({
        'key' : np.arange(n)
    })
    df['type'] = INSERT


    write_out(df, 'seq_insert.txt')


def generate_rand_insert(n):

    df = pd.DataFrame({
        'key' : np.random.randint(RAND_MIN, RAND_MAX, n)
    })
    df['type'] = INSERT

    write_out(df, 'rand_insert.txt')

def generate_rand_insert_with_hc_read(n, nreads):

    df = pd.DataFrame({
        'key' : np.repeat(np.random.randint(RAND_MIN, RAND_MAX, n), 2)
    })
    df['type'] = INSERT
    df['type'].loc[df.index % 2 == 1] = READ

    if nreads < n:
        # remove some reads
        drop_reads =df.loc[df['type'] == READ].sample(n=n-nreads).index
        df.drop(index=drop_reads, inplace=True)

    write_out(df, 'rand_insert_with_hc_read.txt')

def generate_seq_insert_with_hc_read(n, nreads):
    df = pd.DataFrame({
        'key' : np.repeat(np.arange(n), 2)
    })
    df['type'] = INSERT
    df['type'].loc[df.index % 2 == 1] = READ
    if nreads < n:
        # remove some reads
        drop_reads =df.loc[df['type'] == READ].sample(n=n-nreads).index
        df.drop(index=drop_reads, inplace=True)
    

    write_out(df, 'seq_insert_with_hc_read.txt')

def generate_rand_insert_with_read(n, nreads):
    # TODO impl 
    pass



def main(args):
    generate_seq_insert(args.n)
    generate_rand_insert(args.n)
    generate_seq_insert_with_hc_read(args.n, args.nreads)
    # TODO impl and uncomment
    #generate_rand_insert_with_read(args.n, args.nreads)

if __name__ == '__main__':
    main(argp.parse_args(sys.argv[1:]))


