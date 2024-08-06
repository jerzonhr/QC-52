import pandas as pd
import sqlalchemy

file_path = "files/unique_leads.csv"
df = pd.read_csv(file_path)


def main():
    # Your code here
    print(df.count())


if __name__ == "__main__":
    main()
