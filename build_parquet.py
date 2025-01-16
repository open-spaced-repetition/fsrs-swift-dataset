import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
from tqdm.auto import tqdm  # type: ignore
from pathlib import Path


class IdMapper:
    def __init__(self):
        self._mappings = {}

    def get_mapping(self, column_name):
        if column_name not in self._mappings:
            self._mappings[column_name] = {}
        return self._mappings[column_name]

    def factorize(self, series, column_name):
        mapping = self.get_mapping(column_name)
        result = series.map(lambda x: mapping.setdefault(x, len(mapping)))
        return result


def save_to_parquet(df, user_id):
    if df.empty:
        return

    df["user_id"] = user_id
    table = pa.Table.from_pandas(df)
    output_path = Path("./revlogs")

    pq.write_to_dataset(
        table,
        output_path,
        partition_cols=["user_id"],
        existing_data_behavior="delete_matching",
    )

    # rename the file to user_id=xxx
    for file in (output_path / f"user_id={user_id}").glob("*.parquet"):
        new_name = file.with_name("data.parquet")
        file.rename(new_name)


def process_and_save(file_path: Path, id_mapper: IdMapper) -> None:
    df = pd.read_csv(file_path)
    df["review_date"] = pd.to_datetime(df["review_time"] // 1000, unit="s")
    df["real_days"] = pd.DatetimeIndex(
        df["review_date"].dt.floor("D", ambiguous="infer", nonexistent="shift_forward")
    ).to_julian_date()
    df["elapsed_days"] = df.real_days.diff().fillna(0)
    df["elapsed_seconds"] = (df["review_time"].diff().fillna(0) / 1000).astype("int64")
    df["i"] = df.groupby("card_id").cumcount() + 1
    df.loc[df["i"] == 1, "elapsed_days"] = -1
    df.loc[df["i"] == 1, "elapsed_seconds"] = -1
    df["card_id"] = id_mapper.factorize(df["card_id"], "card_id")
    df.sort_values(by="review_time", inplace=True)
    df_to_save = df[
        [
            "card_id",
            "rating",
            "duration",
            "elapsed_days",
            "elapsed_seconds",
        ]
    ].copy()
    save_to_parquet(df_to_save, file_path.stem)


def main() -> None:
    id_mapper = IdMapper()
    for file_path in tqdm(Path("./dataset").glob("*.csv")):
        process_and_save(file_path, id_mapper)


if __name__ == "__main__":
    main()
