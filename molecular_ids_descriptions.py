import csv
import sys
from civicpy import civic

reader = csv.reader(sys.stdin)

for chrom, start, ref, var, molecular_profile_id in reader:
    profile = civic.get_molecular_profile_by_id(molecular_profile_id)