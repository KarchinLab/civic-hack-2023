from civicpy import civic

annotated_mps = list()
molecular_profiles = civic.get_all_molecular_profiles(include_status=["accepted", "submitted"])
for n, mp in enumerate(molecular_profiles):
    # if n > 10:
    #     break
    ev_counts = {
        "accepted": 0,
        "submitted": 0
    }
    for ev in mp.evidence_items:
        if ev.status in {"accepted", "submitted"}:
            ev_counts[ev.status] += 1
    is_and = False
    # print(gq_mp)
    for parsed_name in mp.parsed_name:
        continue
        if parsed_name.type == 'molecular_profile_text_segment':
            print(mp.id, gq_mp)
            if parsed_name.text == 'OR':
                is_and = False
            elif parsed_name.text == 'AND':
                is_and = True
            else:
                print('Unrecognized token text: ' + token['text'])
            break
    if is_and == 'AND':
        continue
    for variant in mp.variants:
        chrom = None
        start = None
        ref = None
        alt = None
        coordinates = variant.coordinates
        # molecular_profile_id = variant.single_variant_molecular_profile
        chrom, start, ref, alt = coordinates.chromosome, coordinates.start, coordinates.reference_bases, coordinates.variant_bases
        if not (ref and alt):
            continue
        annotated_mps.append({
            "chrom": chrom,
            "start": start,
            "ref": ref,
            "alt": alt,
            "mp_id": mp.id,
            "variant_ids": mp.variant_ids,
            "molecular_profile_score": mp.molecular_profile_score,
            "num_acc_eids": ev_counts["accepted"],
            "num_sub_eids": ev_counts["submitted"]
        })
print(len(annotated_mps))
annotated_mps[0]
