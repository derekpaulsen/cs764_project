import pandas as pd
import matplotlib.pyplot as plt
import sys

SCALE = 1000000

WORKLOAD_REP = {
    './workload/rand_insert.txt' : 'Random Insert',
    './workload/seq_insert.txt' : 'Tail Insert',
    './workload/seq_insert_with_hc_read.txt' : 'Tail Insert with Read (50M insert, 10M read)',
    './workload/rand_insert_with_hc_read.txt' : 'Random Insert with Read (50M insert, 10M read)',
    './workload/seq_insert_with_hc_read_50_50.txt' : 'Tail Insert with Read (50M insert, 50M read)',
    './workload/rand_insert_with_hc_read_50_50.txt' : 'Random Insert with Read (50M insert, 50M read)'
}

PAIRS = [
('Random Insert', 'Tail Insert'),
('Random Insert with Read (50M insert, 10M read)', 'Tail Insert with Read (50M insert, 10M read)'),
('Random Insert with Read (50M insert, 50M read)', 'Tail Insert with Read (50M insert, 50M read)'),
]

ALGOR_REP= {
        'baseline' : 'Baseline',
        'RingBufferedBTree' : 'Buffered'
}

def create_pt(d):
    #print(d)
    #print(d.columns)
    nt = d['num_threads'].loc['50%']
    d = d['ops_per_sec'] / SCALE

    return pd.Series({
        'num_threads' : nt,
        'min' : d.loc['min'],
        'max' : d.loc['max'],
        'median' : d.loc['50%']
    })


def create_pts(grp):
    #print(grp)
    return grp.groupby('num_threads')\
            .apply(lambda x : create_pt(x.describe()))

def add_err_line(ax, pts, label):
    x = pts['num_threads']
    y = pts['median']
    low_err = pts['min']
    up_err = pts['max']
    err = [y - low_err, up_err - y]

    ax.errorbar(x, y, yerr = err, fmt='-o', label=label)


def plot_err(df, exp, algors):

    df = df.loc[exp]

    fig, ax = plt.subplots(1)

    for a in algors:
        pts = create_pts(df.loc[a])
        print(pts)
        add_err_line(ax, pts, a)

    ax.legend()
    ax.set_title(exp)
    ax.set_xlabel('# Threads')
    ax.set_ylabel('M ops/sec')
    fig.savefig(f'{exp}.png', bbox_inches='tight')


    plt.show()


def plot_err_2(df, exps, algors):


    fig, axes = plt.subplots(nrows=len(exps))
    for ax, exp in zip(axes, exps):
        for a in algors:
            pts = create_pts(df.loc[exp,a])
            print(pts)
            add_err_line(ax, pts, a)

        ax.legend()
        ax.set_title(exp)
        ax.set_ylabel('M ops/sec')
        ax.set_xticks([4,8,12,16,20,24,28,32])

        ax.set_xlabel('# Threads')
    fig.tight_layout()
    fig.savefig(f'{exp} 2.png')


    plt.show()
def main(fname):
    df = pd.read_json(fname, lines=True)
    df['workload'] = df['workload'].replace(WORKLOAD_REP)
    df['algor'] = df['algor'].replace(ALGOR_REP)

    algors = df.algor.unique()
    workloads = df.workload.unique()

    df = df.set_index(['workload', 'algor'])

    #for wl in workloads:
    #    plot_err(df, wl, algors)

    for p in PAIRS:
        plot_err_2(df, p, algors)

if __name__ == '__main__':
    main(sys.argv[1])
