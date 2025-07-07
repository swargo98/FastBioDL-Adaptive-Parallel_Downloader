# sum the third column of timed_log_download.csv
import csv
def sum_third_column(filename):
    total = 0.0
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 3:  # Ensure there are at least 3 columns
                try:
                    value = float(row[2])  # Convert the third column to float
                    total += value
                except ValueError:
                    print(f"Skipping invalid value: {row[2]}")
    return total

if __name__ == "__main__":
    filename = 'timed_log_download.csv'
    total_sum = sum_third_column(filename)
    print(f"The sum of the third column in '{filename}' is: {total_sum}")