from dagster import asset, Partition, PartitionsDefinition
from datetime import datetime

class YearlyPartitionsDefinition(PartitionsDefinition):
    def __init__(self, start_year, end_year):
        self.start_year = start_year
        self.end_year = end_year

    def get_partitions(self):
        return [Partition(str(year)) for year in range(self.start_year, self.end_year + 1)]

    def get_partition_keys(self):
        return [str(year) for year in range(self.start_year, self.end_year + 1)]