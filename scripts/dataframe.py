import pandas as pd
import glob
import pickle
import os

def set_frame(item):
  # item is a path towards a file
  df = None
  filename, extension = os.path.splitext(item)
  match(extension):
    case '.csv':
      df = pd.read_csv(item)
    case '.parquet':
      df = pd.read_parquet(item)
    case '.xlsx':
      df = pd.read_excel(item)
    case '.html':
      df = pd.read_html(item)[0]
    case '.json':
      df = pd.read_json(item)
    case '.pickle' | '.pkl':
      df = pd.read_pickle(item)
  df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
  return df

def append_files(df, file_list):
  file_list.sort()
  for item in file_list:
    filename, extension = os.path.splitext(item)
    ctr = 1
    match(extension):
      case '.csv':
        df = pd.concat([df, pd.read_csv(item)], ignore_index=True)
      case '.parquet':
        df = pd.concat([df, pd.read_parquet(item)], ignore_index=True)
      case '.xlsx':
        df = pd.concat([df, pd.read_excel(item)], ignore_index=True)
      case '.html':
        df = pd.concat([df, pd.read_html(item)[0]], ignore_index=True)
      case '.json':
        df = pd.concat([df, pd.read_json(item)], ignore_index=True)
      case '.pickle' | '.pkl':
        df = pd.concat([df, pd.read_pickle(item)], ignore_index=True)
    print("Read",ctr,"file/s")
    ctr = ctr+1

  df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
  return df