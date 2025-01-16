# FSRS Swift Dataset

The original dataset is hosted on [Networks-Learning/spaced-selection](https://github.com/Networks-Learning/spaced-selection).

This repository contains the code to extract reviews from the dataset, group them by user, and save them to parquet files.

The parquet files can be used for [srs-benchmark](https://github.com/open-spaced-repetition/srs-benchmark).

## Usage

1. Download the dataset from [here](https://www.google.com/url?q=https://owncloud.mpi-sws.org/index.php/s/Pgn3Q9N592Z8MgZ&sa=D&source=hangouts&ust=1619105903052000&usg=AFQjCNEzKdG-8rL8eVnbQV7yl0ZXgVxetQ).
2. Unzip the file and move the `stats-20191220-20200731` folder to the root of this repository.
3. Run `python group_user_reviews.py` to group reviews by user and save them to csv files.
4. Run `python build_parquet.py` to build parquet files.
