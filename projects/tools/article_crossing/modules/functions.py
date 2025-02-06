import os
from re import sub
from itertools import combinations

import pandas as pd
from loguru import logger


def create_dataframe(path: str) -> pd.DataFrame:
    return pd.read_excel(
        path,
        header=None,
        names=['origin_id', 'origin_brand', 'analog_id', 'analog_brand'],
        dtype='str'
    )

def check_total_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    duplicates = dataframe.duplicated().sum()
    logger.info(f"Found {duplicates} full duplicates, removing them")
    return dataframe.drop_duplicates()

def chech_analog_duplicates(dataframe: pd.DataFrame, input_callback):
    logger.debug("Checking for analog duplicates")
    duplicated_analogs = dataframe.value_counts(subset=['analog_id', 'analog_brand']).reset_index().query('count > 1')
    duplicated_analogs['KEY'] = duplicated_analogs['analog_id'] + ' - ' + duplicated_analogs['analog_brand']
    
    temp_df = dataframe.copy()
    temp_df['KEY'] = dataframe['analog_id'] + ' - ' + dataframe['analog_brand']
    temp_df = temp_df[temp_df['KEY'].isin(duplicated_analogs['KEY'])].sort_values(
        ['KEY', 'origin_id', 'origin_brand']
    )
    
    logger.warning(f"Found {len(temp_df)} analog duplicates")
    if len(temp_df) > 0:
        logger.debug(f"Duplicate details:\n{temp_df.to_string()}")
        if not input_callback():
            logger.error("Processing aborted by user")
            raise Exception("Processing aborted by user")
    return dataframe

def clear_punctuation_symbols(dataframe: pd.DataFrame) -> pd.DataFrame:
    logger.debug("Cleaning punctuation symbols")
    for col in ['origin_id', 'analog_id']:
        dataframe[col] = dataframe[col].apply(lambda x: sub(r'[^A-Za-z0-9]+', '', x))
    return dataframe

def create_key_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    logger.debug("Creating key columns")
    dataframe['ORIGIN_KEY'] = dataframe['origin_id'] + ' - ' + dataframe['origin_brand']
    dataframe['ANALOG_KEY'] = dataframe['analog_id'] + ' - ' + dataframe['analog_brand']
    return dataframe

def create_source_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.loc[:, "source_id_brand"] = [
        f"{brand}#{article}"
        for brand, article in zip(
            dataframe.loc[:, "origin_brand"], dataframe.loc[:, "origin_id"]
        )
    ]
    return dataframe

def main(path: str, input_callback):
    logger.info("Starting main processing")
    df = (create_dataframe(path)
          .pipe(check_total_duplicates)
          .pipe(chech_analog_duplicates, input_callback)
          .pipe(clear_punctuation_symbols)
          .pipe(create_key_columns)
          .pipe(create_source_column))
    
    logger.debug("Processing unique combinations")
    origin_unique = set(df['ORIGIN_KEY'])
    result_df = pd.DataFrame(columns=['source_id_brand', 'origin_id', 'origin_brand', 'analog_id', 'analog_brand'])
    
    for pair in origin_unique:
        temp_df = df.query('ORIGIN_KEY == @pair')
        analogs = set(temp_df['ANALOG_KEY'])
        
        if len(analogs) < 2:
            continue
            
        combos = pd.DataFrame(combinations(analogs, 2))
        combos[['origin_id', 'origin_brand']] = combos[0].str.split(pat=' - ', n=1, expand=True)
        combos[['analog_id', 'analog_brand']] = combos[1].str.split(pat=' - ', n=1, expand=True)
        combos['source_id_brand'] = pair
        combos = combos.drop(labels=[0, 1], axis=1)
        combos.columns = ['origin_id', 'origin_brand', 'analog_id', 'analog_brand', 'source_id_brand']
        
        result_df = pd.concat([result_df, combos])
    
    final_df = (pd.concat([df.drop(['ORIGIN_KEY', 'ANALOG_KEY'], axis=1), result_df])
                .drop_duplicates()
                .reset_index(drop=True))
    
    output_path = os.path.join(os.path.dirname(path), 'result.xlsx')
    logger.info(f"Saving results to {output_path}")
    final_df.to_excel(output_path, index=False)
    logger.success(f"Results successfully saved to: {output_path}")
