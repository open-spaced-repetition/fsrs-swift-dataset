import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm  # type: ignore
from concurrent.futures import ThreadPoolExecutor


def process_group(user_group):
    user, group = user_group
    if len(group) < 1000:
        return
    if len(group[group["i"] == 2]) < 100:
        return
    group[["card_id", "review_time", "i", "rating", "duration"]].sort_values(
        by=["card_id", "review_time"]
    ).to_csv(f"./dataset/{user}.csv", index=False)


if __name__ == "__main__":
    Path("./dataset").mkdir(exist_ok=True, parents=True)
    dir = Path("./stats-20191220-20200731")
    df = pd.concat([pd.read_csv(f) for f in tqdm(dir.glob("*.csv"))])
    df["review_time"] = (
        pd.to_datetime(
            df["datecreated"],
            format="%a %b %d %Y %H:%M:%S GMT%z (UTC)",
            errors="coerce",
            utc=True,
        ).astype(int)
        // 10**6
    )
    df["card_id"] = df["question"]
    df["rating"] = df["correct"].map({0: 1, 1: 3})
    df["duration"] = (df["seconds"].astype(float) * 1000).astype(int)
    df["i"] = df["count"]

    grouped = df.groupby("user")
    with ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(process_group, grouped), total=len(grouped)))
