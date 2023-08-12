import json

import requests
import csv
import sys
import sqlite3
import logging
from datetime import datetime
import time
import os


# Database Helper
class CivicDB:
    """Utility for interacting with CIVIC Databases"""
    _insert_gene_sql = '''INSERT INTO civic_gene(
            id,
            name,
            description,
            aliases
        ) VALUES (?, ?, ?, ?)'''
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
        self.cursor.execute('DROP TABLE IF EXISTS civic_gene;')
        self.cursor.execute('''CREATE TABLE civic_gene (
            id INT,
            name TEXT,
            description TEXT,
            aliases TEXT
        )''')

    def insert_gene(self, data):
        self.cursor.execute(self._insert_gene_sql, data)

    def create_index(self):
        self.cursor.execute('CREATE INDEX civic_gene_index ON civic_gene (name)')


class CivicBuilder:
    # Constants
    logger = logging.getLogger('build_civic')
    base_download_url = 'https://civicdb.org/downloads'
    graphql_url = 'https://civicdb.org/api/graphql'
    gene_query = """query gene($id: Int!) {
        gene(id: $id) {
            id,
            description,
            name,
            geneAliases
          }
        }
    """

    def __init__(self):
        self.description_idx = 0
        self.id_idx = 0
        self.name_idx = 0
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler_format = '%(asctime)s | %(levelname)s: %(message)s'
        console_handler.setFormatter(logging.Formatter(console_handler_format))
        file_handler = logging.FileHandler('build_civic_gene.log', 'w', 'utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(console_handler_format))
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    # Helper method
    def get_current_month_variant_file_url(self):
        """Get the file url for current monthly variant snapshot. e.g. '01-Mar-2023-VariantSummaries.tsv'"""
        now = datetime.now()
        first = now.replace(day=1)
        formatted = first.strftime('%d-%b-%Y')
        file_name = 'GeneSummaries'
        return f'{self.base_download_url}/{formatted}/{formatted}-{file_name}.tsv'


    def get_gene_data(self, gene_snapshot: list, gene_json: dict) -> tuple:
        """Extract data for a single variant from the snapshot and api results"""
        gene_id = gene_snapshot[self.id_idx]
        name = gene_snapshot[self.name_idx]
        description = gene_snapshot[self.description_idx]
        alias_list = gene_json['data']['gene']['geneAliases']
        aliases = ",".join(alias_list)
        return gene_id, name, description, aliases

    def main(self):
        # Update script
        start_time = time.time()
        self.logger.info("Civic Gene Auto Update Started: %s" % time.asctime(time.localtime(start_time)))
        full_url = self.get_current_month_variant_file_url()
        self.logger.info(f'Current monthly filename: {repr(full_url)}')

        total = 0
        with requests.Session() as s, CivicDB('civic_gene.sqlite') as db:
            db.create_variant_table()
            file = s.get(full_url, timeout=120)
            reader = csv.reader(file.text.splitlines(), delimiter='\t')
            lines = list(reader)
            # find the index numbers of the columns we're interested in
            for i, column_name in enumerate(lines[0]):
                if column_name == 'gene_id':
                    self.id_idx = i
                elif column_name == 'name':
                    self.name_idx = i
                elif column_name == 'description':
                    self.description_idx = i

            for gene in lines[1:]:
                gene_id = gene[self.id_idx]
                gene_gql = {
                    'query': self.gene_query,
                    'operation_name': f'gene_{gene_id}',
                    'variables': f'{{ "id": {gene_id} }}'
                }
                g_file = s.post(self.graphql_url, json=gene_gql, timeout=30)
                g_json = json.loads(g_file.text)
                gene_data = self.get_gene_data(gene_snapshot=gene, gene_json=g_json)
                self.logger.info(repr(gene_data))
                total += 1
                db.insert_gene(gene_data)
            db.create_index()

        stop_time = time.time()
        self.logger.info("Finished: %s" % time.asctime(time.localtime(stop_time)))
        runtime = stop_time - start_time
        self.logger.info("Runtime: %6.3f" % runtime)
        self.logger.info(f'Records inserted: {total}')


if __name__ == '__main__':
    civic_builder = CivicBuilder()
    civic_builder.main()

    if os.path.exists('civic_gene.sqlite'):
        os.system('~/miniconda3/bin/python3 tester.py "/local/home/kanderson/civic_gene" "mlarsen@potomacitgroup.com" "kmoad@potomacitgroup.com" "mstucky@potomacitgroup.com" "kanderson@potomacitgroup.com"')
    else:
        print('The database was not created')
