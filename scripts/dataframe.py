import pandas as pd
import glob
import pickle
import os
import shutil
from datetime import datetime

def set_frame(item):
    """Read a file into a DataFrame, handling empty files and errors gracefully"""
    df = None
    filename, extension = os.path.splitext(item)

    try:
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

        # Remove unnamed columns if DataFrame is not empty
        if df is not None and not df.empty:
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # If file resulted in empty DataFrame, return empty DataFrame
        if df is None or df.empty:
            print(f"Warning: {item} is empty or has no data. Creating empty DataFrame.")
            return pd.DataFrame()

    except pd.errors.EmptyDataError:
        print(f"Warning: {item} is empty. Creating empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {item}: {e}. Creating empty DataFrame.")
        return pd.DataFrame()

    return df

def append_files(df, file_list):
    """Append multiple files to a DataFrame, handling empty files and errors gracefully"""
    file_list.sort()
    ctr = 1

    for item in file_list:
        filename, extension = os.path.splitext(item)
        temp_df = None

        try:
            match(extension):
                case '.csv':
                    temp_df = pd.read_csv(item)
                case '.parquet':
                    temp_df = pd.read_parquet(item)
                case '.xlsx':
                    temp_df = pd.read_excel(item)
                case '.html':
                    temp_df = pd.read_html(item)[0]
                case '.json':
                    temp_df = pd.read_json(item)
                case '.pickle' | '.pkl':
                    temp_df = pd.read_pickle(item)

            # Only concatenate if temp_df is not empty
            if temp_df is not None and not temp_df.empty:
                df = pd.concat([df, temp_df], ignore_index=True)
                print(f"Read {ctr} file(s): {item}")
            else:
                print(f"Warning: Skipped empty file {item}")

        except pd.errors.EmptyDataError:
            print(f"Warning: Skipped empty file {item}")
        except Exception as e:
            print(f"Error reading {item}: {e}. Skipping file.")

        ctr += 1

    # Remove unnamed columns if df is not empty
    if not df.empty:
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    return df

def archive_files(file_list, dataset_path):
    """
    Move processed files to a /loaded folder within their department directory.
    Creates /loaded folder if it doesn't exist.

    Args:
        file_list: List of file paths that were processed
        dataset_path: Base dataset path
    """
    if not file_list:
        print("No files to archive.")
        return

    for file_path in file_list:
        try:
            # Get the directory containing the file
            file_dir = os.path.dirname(file_path)
            file_name = os.path.basename(file_path)

            # Create /loaded folder in the same directory
            loaded_dir = os.path.join(file_dir, "loaded")
            os.makedirs(loaded_dir, exist_ok=True)

            # Move file to /loaded folder
            destination = os.path.join(loaded_dir, file_name)

            # If file already exists in loaded folder, rename with timestamp
            if os.path.exists(destination):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = os.path.splitext(file_name)
                destination = os.path.join(loaded_dir, f"{name}_{timestamp}{ext}")

            shutil.move(file_path, destination)
            print(f"Archived: {file_path} -> {destination}")

        except Exception as e:
            print(f"Error archiving {file_path}: {e}")

def extract_with_archiving(pattern, output_path, dataset_path):
    """
    Helper function to extract files, handle empty cases, and archive processed files.
    Creates empty parquet if no files found or all files are empty.

    Args:
        pattern: Glob pattern to find files
        output_path: Path where to save the parquet file
        dataset_path: Base dataset path for archiving

    Returns:
        DataFrame (empty if no files found or all files empty)
    """
    files = glob.glob(pattern)

    # If no files found, create empty parquet
    if not files:
        print(f"Warning: No files found matching pattern {pattern}")
        print(f"Creating empty parquet at {output_path}")
        pd.DataFrame().to_parquet(output_path, index=False)
        return pd.DataFrame()

    # Sort files
    files.sort()
    print(f"Found {len(files)} file(s) matching pattern {pattern}")

    # Read first file
    df = set_frame(files[0])

    # Append remaining files if any
    if len(files) > 1:
        files_to_append = files[1:]
        df = append_files(df, files_to_append)

    # Save to parquet (even if empty)
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")

    # Archive all processed files
    archive_files(files, dataset_path)

    return df
