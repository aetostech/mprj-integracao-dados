from tqdm import tqdm

from candidatura import ETLCandidatura
from despesa import ETLDespesa
from doacao import ETLDoacao

raw_dir = 'data/raw'
processed_dir = 'data/processed'
year = 2022
objs = [ETLCandidatura(year, raw_dir, processed_dir),
        ETLDespesa(year, raw_dir, processed_dir),
        ETLDoacao(year, raw_dir, processed_dir)
        ]

for etl in tqdm(objs, leave=False, desc='executing modules'):
    etl.read_data()
    etl.transform_data()
    etl.save_data()
