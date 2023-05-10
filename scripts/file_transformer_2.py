"""
This module cleans up after messy write to csv from the cmd
"""
import os
import sys

# specify input and output file paths
INPUT = sys.argv[1]

# read input file and write output file
with open('output/out.csv', 'w') as f_out:
    with open(INPUT, 'r') as f_in:
        # skip header
        header = next(f_in)
        f_out.write(header)
        for line in f_in:
            # split line by comma
            line_parts = line.strip().split(',')
            # join all items except last two as a single string
            NEW_LINE = '-'.join(line_parts[:-2]).strip()
            # add the last item to the new line
            NEW_LINE += ',' + line_parts[-2].strip() + ',' + line_parts[-1].strip()
            # write new line to output file
            f_out.write(NEW_LINE + '\n')

# remove old file
os.remove(INPUT)

# rename output file as old file name
os.rename('output/out.csv', INPUT)
