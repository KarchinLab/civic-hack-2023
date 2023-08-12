from civicpy import civic
import csv
import sys

writer = csv.writer(sys.stdout)

profile = civic.get_all_molecular_profiles(include_status=["accepted", "submitted"])
for p in profile:
    for variant in p.variants:
        coordinates = variant.coordinates

        profile_ids = [ prof.id for prof in variant.molecular_profiles] 
        molecular_profile_id = variant.single_variant_molecular_profile
        chrom, start, ref, var = coordinates.chromosome, coordinates.start, coordinates.reference_bases, coordinates.variant_bases
        if not (ref and var):
            continue
        
        writer.writerow([chrom, start, ref, var, p.id])

