import requests
import csv
import json
import sys
from pyliftover import LiftOver
import sqlite3
import logging
from datetime import datetime
import time
import os


# Database Helper
class CivicDB:
    """Utility for interacting with CIVIC Databases"""
    _insert_variant_sql = '''INSERT INTO civic(
            id,
            chromosome,
            start,
            reference_build,
            reference_base,
            variant_base,
            description,
            molecular_profile_score,
            diseases
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    _logger = logging.getLogger('CivicDB')

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self._logger.info(f'Connecting to SQLite DB at {repr(self.path)}')
        self.db = sqlite3.connect(self.path)
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, type, value, traceback):
        self.db.commit()
        self.cursor.close()
        self.db.close()

    def create_variant_table(self):
        self.cursor.execute('DROP TABLE IF EXISTS civic;')
        self.cursor.execute('''CREATE TABLE civic (
            id INT,
            chromosome TEXT,
            start TEXT,
            reference_base TEXT,
            reference_build TEXT,
            variant_base TEXT,
            description TEXT,
            molecular_profile_score REAL,
            diseases TEXT
        )''')

    def insert_variant(self, data):
        self.cursor.execute(self._insert_variant_sql, data)

    def create_index(self):
        self.cursor.execute('CREATE INDEX civic_index ON civic (start, reference_base, variant_base)')


class CivicBuilder:
    # Constants
    logger = logging.getLogger('build_civic')
    lifter = LiftOver('hg19', 'hg38')
    base_download_url = 'https://civicdb.org/downloads'
    graphql_url = 'https://civicdb.org/api/graphql'
    variant_query = """query variant($id: Int!) {
      variant(id: $id) {
        singleVariantMolecularProfile {
          description
          molecularProfileScore
          evidenceItems {
            nodes {
              disease {
                name
                id
              }
            }
          }
        }
        id
        link
        name
        referenceBases
        variantBases
        referenceBuild
        primaryCoordinates {
          chromosome
          start
          stop
          representativeTranscript
        }
      }
    }
    """

    def __init__(self):
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler_format = '%(asctime)s | %(levelname)s: %(message)s'
        console_handler.setFormatter(logging.Formatter(console_handler_format))
        file_handler = logging.FileHandler('build_civic.log', 'w', 'utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(console_handler_format))
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    # Helper methods
    def deep_get(self, obj: dict, *keys: str):
        temp = obj
        for key in keys:
            try:
                temp = temp[key]
            except (KeyError, TypeError):
                return None
        return temp

    def get_diseases(self, evidence: list) -> str:
        """Format a list of evidence objects into a comma-separated string of disease names"""
        disease_names = map(lambda e: self.deep_get(e, 'disease', 'name'), evidence)
        unique = {name for name in disease_names if name is not None}
        ordered = sorted(unique)
        return ", ".join(ordered)

    def normalize_position(self, reference_build: str, chrom: str, start_pos: str) -> tuple | None:
        """Convert from hg19 to hg38"""
        # only translate hg19, otherwise fail
        if reference_build != 'GRCH37' and reference_build != 'hg19':
            return None, None, None
        start_coords = self.lifter.convert_coordinate(f'chr{chrom}', start_pos)
        ref = 'hg38'
        if start_coords is not None and len(start_coords) > 0:
            start = start_coords[0][1]
            chromosome = start_coords[0][0]
        else:
            return None, None, None
        return ref, chromosome, start

    def get_variant_data(self, variant_snapshot: list, variant_json: dict) -> tuple | None:
        """Extract data from a single variant from the monthly snapshot and data from the graphql query"""
        evidence = self.deep_get(variant_json, 'data', 'variant', 'singleVariantMolecularProfile', 'evidenceItems', 'nodes')
        chromosome = self.deep_get(variant_json, 'data', 'variant', 'primaryCoordinates', 'chromosome')
        start = self.deep_get(variant_json, 'data', 'variant', 'primaryCoordinates', 'start')
        stop = self.deep_get(variant_json, 'data', 'variant', 'primaryCoordinates', 'stop')
        # skip bad data
        if chromosome is None or start is None:
            return None

        reference_base = self.deep_get(variant_json, 'data', 'variant', 'referenceBases')
        variant_base = self.deep_get(variant_json, 'data', 'variant', 'variantBases')
        # skip structural variants and add '-' character for insertions/deletions
        if reference_base is None and variant_base is None:
            return None
        elif reference_base is None:
            reference_base = '-'
        elif variant_base is None:
            variant_base = '-'

        reference_build = self.deep_get(variant_json, 'data', 'variant', 'referenceBuild')
        # Convert old coordinates to hg38
        if reference_build != 'GRCH38' or reference_build != 'hg38':
            reference_build, chromosome, start = self.normalize_position(
                reference_build=reference_build,
                chrom=chromosome,
                start_pos=start)
        if reference_build is None:
            return None

        mps = self.deep_get(variant_json, 'data', 'variant', 'singleVariantMolecularProfile', 'molecularProfileScore')
        description = self.deep_get(variant_json, 'data', 'variant', 'singleVariantMolecularProfile', 'description')
        return (
            variant_snapshot[0],
            chromosome,
            start,
            reference_build,
            reference_base,
            variant_base,
            description,
            mps,
            self.get_diseases(evidence)
        )

    def get_current_month_variant_file_url(self):
        """Get the file url for current monthly variant snapshot. e.g. '01-Mar-2023-VariantSummaries.tsv'"""
        now = datetime.now()
        first = now.replace(day=1)
        formatted = first.strftime('%d-%b-%Y')
        file_name = 'VariantSummaries'
        return f'{self.base_download_url}/{formatted}/{formatted}-{file_name}.tsv'

    def main(self):
        # Update script
        start_time = time.time()
        self.logger.info("Civic Auto Update Started: %s" % time.asctime(time.localtime(start_time)))
        full_url = self.get_current_month_variant_file_url()
        self.logger.info(f'Current monthly filename: {repr(full_url)}')

        total = 0
        with requests.Session() as s, CivicDB('civic.sqlite') as db:
            db.create_variant_table()
            file = s.get(full_url, timeout=120)
            reader = csv.reader(file.text.splitlines(), delimiter='\t')
            lines = list(reader)
            for i, column_name in enumerate(lines[0]):
                if column_name == 'variant_id':
                    id_idx = i

            for variant in lines[1:]:
                v_id = variant[id_idx]
                variant_gql = {
                    'query': self.variant_query,
                    'operation_name': f'variant_{v_id}',
                    'variables': f'{{ "id": {v_id} }}'
                }
                v_file = s.post(self.graphql_url, json=variant_gql, timeout=30)
                v_json = json.loads(v_file.text)
                variant_data = self.get_variant_data(variant_snapshot=variant, variant_json=v_json)
                if variant_data is not None:
                    self.logger.info(repr(variant_data))
                    total += 1
                    db.insert_variant(variant_data)
            db.create_index()

        stop_time = time.time()
        self.logger.info("Finished: %s" % time.asctime(time.localtime(stop_time)))
        runtime = stop_time - start_time
        self.logger.info("Runtime: %6.3f" % runtime)
        self.logger.info(f'Records inserted: {total}')


if __name__ == '__main__':
    civic_builder = CivicBuilder()
    civic_builder.main()

    if os.path.exists('civic.sqlite'):
        os.system('~/miniconda3/bin/python3 tester.py "/local/home/kanderson/civic" "mlarsen@potomacitgroup.com" "kmoad@potomacitgroup.com" "mstucky@potomacitgroup.com" "kanderson@potomacitgroup.com"')
    else:
        print('The database was not created')
